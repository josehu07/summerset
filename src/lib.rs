mod replicator;
mod statemach;

use std::sync::Mutex;

use statemach::{Command, CommandResult, StateMachine};

// TODO: implement this
#[derive(Debug, Default)]
pub struct SummersetError {}

#[derive(Debug, Default)]
pub struct SummersetNode {
    kvlocal: Mutex<StateMachine>, // server handle is already Arc
}

impl SummersetNode {
    pub fn new() -> Self {
        SummersetNode {
            kvlocal: Mutex::new(StateMachine::new()),
        }
    }

    pub fn handle_get(
        &self,
        key: &str,
    ) -> Result<Option<String>, SummersetError> {
        let cmd = Command::Get { key: key.into() };

        // TODO: invoke replicator and block until safe execution point

        // take the lock on local state machine
        let kvlocal_guard = self.kvlocal.lock();
        if kvlocal_guard.is_err() {
            return Err(SummersetError {});
        }

        // execute command on local state machine
        match kvlocal_guard.unwrap().execute(&cmd) {
            CommandResult::GetResult { value } => Ok(value),
            _ => Err(SummersetError {}),
        }
    }

    pub fn handle_put(
        &self,
        key: &str,
        value: &str,
    ) -> Result<Option<String>, SummersetError> {
        let cmd = Command::Put {
            key: key.into(),
            value: value.into(),
        };

        // TODO: invoke replicator and block until safe execution point

        // take the lock on local state machine
        let kvlocal_guard = self.kvlocal.lock();
        if kvlocal_guard.is_err() {
            return Err(SummersetError {});
        }

        // execute command on local state machine
        match kvlocal_guard.unwrap().execute(&cmd) {
            CommandResult::PutResult { old_value } => Ok(old_value),
            _ => Err(SummersetError {}),
        }
    }
}
