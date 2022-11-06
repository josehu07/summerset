use std::collections::HashMap;

#[derive(Clone)]
pub enum Command {
    Get { key: String },
    Put { key: String, value: String },
}

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
