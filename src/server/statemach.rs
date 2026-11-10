//! Summerset server state machine module implementation.

use std::collections::{BTreeMap, HashMap};

use get_size::GetSize;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use wincode::{SchemaRead, SchemaWrite};

use crate::client::ClientId;
use crate::server::{ReplicaId, RequestId};
use crate::utils::SummersetError;

/// Command ID type.
pub type CommandId = u64;

/// Identity of the client request a command comes from, so that a request
/// that made it into the log more than once (e.g., retried after a timeout)
/// takes effect only once.
pub type ReqOrigin = (ClientId, RequestId);

/// Maximum number of client sessions retained, evicting the oldest clients.
const MAX_SESSIONS_COUNT: usize = 1 << 20;

/// Maximum total size of the results retained in sessions.
const MAX_SESSIONS_BYTES: usize = 256 << 20;

/// Session of a client: its latest executed write request, whose result is
/// given back to duplicates of it.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, SchemaWrite, SchemaRead, GetSize)]
pub struct Session {
    /// ID of the request.
    pub req: RequestId,

    /// ID of the command that executed it.
    pub cmd_id: CommandId,

    /// Result of its execution.
    pub result: CommandResult,
}

/// Command to the state machine.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, SchemaWrite, SchemaRead, GetSize)]
pub enum Command {
    /// Get the value of given key.
    Get { key: String },

    /// Put a new value into key.
    Put { key: String, value: String },
}

impl Command {
    /// Is the command type read-only? If so, returns the key queried.
    #[inline]
    #[must_use]
    pub fn read_only(&self) -> Option<&String> {
        if let Command::Get { key } = self {
            Some(key)
        } else {
            None
        }
    }

    /// Is the command non-read-only? If so, returns the key updated.
    #[inline]
    #[must_use]
    pub fn write_key(&self) -> Option<&String> {
        if let Command::Put { key, .. } = self {
            Some(key)
        } else {
            None
        }
    }
}

/// Command execution result returned by the state machine.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, SchemaWrite, SchemaRead, GetSize)]
pub enum CommandResult {
    /// `Some(value)` if key is found in state machine, else `None`.
    Get { value: Option<String> },

    /// `Some(old_value)` if key was in state machine, else `None`.
    Put { old_value: Option<String> },
}

impl CommandResult {
    /// Is the command type read-only?
    #[inline]
    #[must_use]
    pub fn read_only(&self) -> bool {
        matches!(self, CommandResult::Get { .. })
    }
}

/// State is simply a `HashMap` from `String` key -> `String` value.
type State = HashMap<String, String>;

/// Filter of client sessions by the ID of the command last updating them.
type SessionFilter = Box<dyn Fn(CommandId) -> bool + Send>;

/// Task for the executor, in submission order.
enum ExecTask {
    /// A command to execute.
    Cmd(CommandId, Command, Option<ReqOrigin>),

    /// A query of the client sessions passing a filter (as of after all
    /// earlier commands).
    GetSessions(SessionFilter, oneshot::Sender<Vec<(ClientId, Session)>>),

    /// Client sessions to restore (e.g., from a snapshot).
    LoadSessions(Vec<(ClientId, Session)>),
}

/// The local volatile state machine, which is simply an in-memory `HashMap`.
pub(crate) struct StateMachine {
    /// My replica ID.
    _me: ReplicaId,

    /// Sender side of the exec channel.
    tx_exec: mpsc::UnboundedSender<ExecTask>,

    /// Receiver side of the ack channel.
    rx_ack: mpsc::UnboundedReceiver<(CommandId, CommandResult)>,

    /// Join handle of the executor task. The state `HashMap` is owned by this
    /// task.
    _executor_handle: JoinHandle<()>,
}

// StateMachine public API implementation
impl StateMachine {
    /// Creates a new state machine with one executor task. Spawns the
    /// executor task. Creates an exec channel for submitting commands to the
    /// state machine and an ack channel for getting results.
    #[allow(clippy::unused_async)]
    pub(crate) async fn new_and_setup(me: ReplicaId) -> Result<Self, SummersetError> {
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

    /// Submits a command by sending it to the exec channel. A write from a
    /// client request takes effect only once per request.
    pub(crate) fn submit_cmd(
        &mut self,
        id: CommandId,
        cmd: Command,
        origin: Option<ReqOrigin>,
    ) -> Result<(), SummersetError> {
        self.tx_exec
            .send(ExecTask::Cmd(id, cmd, origin))
            .map_err(SummersetError::msg)
    }

    /// Gets the client sessions last updated by commands whose IDs pass
    /// `filter`, as of after all commands submitted so far.
    pub(crate) async fn get_sessions(
        &mut self,
        filter: impl Fn(CommandId) -> bool + Send + 'static,
    ) -> Result<Vec<(ClientId, Session)>, SummersetError> {
        let (tx, rx) = oneshot::channel();
        self.tx_exec
            .send(ExecTask::GetSessions(Box::new(filter), tx))
            .map_err(SummersetError::msg)?;
        rx.await.map_err(SummersetError::msg)
    }

    /// Restores client sessions before commands submitted from now on.
    pub(crate) fn load_sessions(
        &mut self,
        sessions: Vec<(ClientId, Session)>,
    ) -> Result<(), SummersetError> {
        self.tx_exec
            .send(ExecTask::LoadSessions(sessions))
            .map_err(SummersetError::msg)
    }

    /// Waits for the next execution result by receiving from the ack channel.
    pub(crate) async fn get_result(
        &mut self,
    ) -> Result<(CommandId, CommandResult), SummersetError> {
        if let Some((id, result)) = self.rx_ack.recv().await {
            Ok((id, result))
        } else {
            logged_err!("ack channel has been closed")
        }
    }

    /// Try to get the next execution result using `try_recv()`.
    #[allow(dead_code)]
    pub(crate) fn try_get_result(&mut self) -> Result<(CommandId, CommandResult), SummersetError> {
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
        origin: Option<ReqOrigin>,
    ) -> Result<(Vec<(CommandId, CommandResult)>, CommandResult), SummersetError> {
        self.submit_cmd(id, cmd, origin)?;
        let mut old_results = vec![];
        loop {
            let (this_id, result) = self.get_result().await?;
            if this_id == id {
                return Ok((old_results, result));
            }
            old_results.push((this_id, result));
        }
    }
}

/// `StateMachine` command executor task.
struct StateMachineExecutorTask {
    rx_exec: mpsc::UnboundedReceiver<ExecTask>,
    tx_ack: mpsc::UnboundedSender<(CommandId, CommandResult)>,

    /// State is ultimately just a key-value `HashMap`.
    state: State,

    /// Map from client ID -> its session, ordered from the oldest client.
    sessions: BTreeMap<ClientId, Session>,

    /// Total size of the results retained in sessions.
    sessions_bytes: usize,
}

impl StateMachineExecutorTask {
    /// Creates the command executor task.
    fn new(
        rx_exec: mpsc::UnboundedReceiver<ExecTask>,
        tx_ack: mpsc::UnboundedSender<(CommandId, CommandResult)>,
    ) -> Self {
        StateMachineExecutorTask {
            rx_exec,
            tx_ack,
            state: State::new(),
            sessions: BTreeMap::new(),
            sessions_bytes: 0,
        }
    }

    /// Executes a command, unless it is a write duplicating one of its
    /// client's latest executed request or earlier ones. Writes of a client
    /// enter the (linear) log in request order, except for such duplicates,
    /// so this is deterministic across replicas executing the same log.
    fn execute_once(
        &mut self,
        id: CommandId,
        cmd: &Command,
        origin: Option<ReqOrigin>,
    ) -> CommandResult {
        let Some((client, req)) = origin.filter(|_| cmd.write_key().is_some()) else {
            return Self::execute(&mut self.state, cmd);
        };
        if let Some(session) = self.sessions.get(&client)
            && req <= session.req
        {
            // a duplicate of the latest one gets the original result; for an
            // older one, the client has seen its reply and moved on already
            return session.result.clone();
        }

        let result = Self::execute(&mut self.state, cmd);
        self.record_session(
            client,
            Session {
                req,
                cmd_id: id,
                result: result.clone(),
            },
        );
        result
    }

    /// Executes given command on the state machine state.
    /// This is a non-method function to make tests easier to write.
    fn execute(state: &mut State, cmd: &Command) -> CommandResult {
        match cmd {
            Command::Get { key } => CommandResult::Get {
                value: state.get(key).cloned(),
            },
            Command::Put { key, value } => CommandResult::Put {
                old_value: state.insert(key.clone(), value.clone()),
            },
        }
    }

    /// Records a client's session, evicting the oldest clients' sessions once
    /// beyond the retention bounds.
    fn record_session(&mut self, client: ClientId, session: Session) {
        self.sessions_bytes += session.result.get_size();
        if let Some(old) = self.sessions.insert(client, session) {
            self.sessions_bytes -= old.result.get_size();
        }
        while self.sessions.len() > MAX_SESSIONS_COUNT || self.sessions_bytes > MAX_SESSIONS_BYTES {
            let (_, old) = self
                .sessions
                .pop_first()
                .expect("sessions should be non-empty");
            self.sessions_bytes -= old.result.get_size();
        }
    }

    /// Starts the command executor task loop.
    async fn run(&mut self) {
        pf_debug!("executor task spawned");

        while let Some(task) = self.rx_exec.recv().await {
            match task {
                ExecTask::Cmd(id, cmd, origin) => {
                    let res = self.execute_once(id, &cmd, origin);
                    // pf_trace!("executed {:?}", cmd);

                    if let Err(e) = self.tx_ack.send((id, res)) {
                        pf_error!("error sending to tx_ack: {}", e);
                    }
                }
                ExecTask::GetSessions(filter, tx) => {
                    // copy out only those asked for, as there may be many
                    let sessions = self
                        .sessions
                        .iter()
                        .filter(|(_, s)| filter(s.cmd_id))
                        .map(|(&c, s)| (c, s.clone()))
                        .collect();
                    if tx.send(sessions).is_err() {
                        pf_error!("error sending sessions");
                    }
                }
                ExecTask::LoadSessions(sessions) => {
                    for (client, session) in sessions {
                        self.record_session(client, session);
                    }
                }
            }
        }

        // channel gets closed and no messages remain
        pf_debug!("executor task exited");
    }
}

#[cfg(test)]
mod tests {
    use rand::RngExt;
    use rand::seq::IndexedRandom;

    use super::*;

    #[test]
    fn get_empty() {
        let mut state = State::new();
        assert_eq!(
            StateMachineExecutorTask::execute(&mut state, &Command::Get { key: "Jose".into() }),
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
            StateMachineExecutorTask::execute(&mut state, &Command::Get { key: "Jose".into() }),
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
        rand::rng()
            .sample_iter(&rand::distr::Alphanumeric)
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
                (*keys.choose(&mut rand::rng()).unwrap()).into()
            } else {
                "nonexist!".into()
            };
            assert_eq!(
                StateMachineExecutorTask::execute(&mut state, &Command::Get { key: key.clone() }),
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
            None,
        )?;
        sm.submit_cmd(
            1,
            Command::Put {
                key: "Jose".into(),
                value: "180".into(),
            },
            None,
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
            None,
        )?;
        sm.submit_cmd(
            1,
            Command::Put {
                key: "Jose".into(),
                value: "180".into(),
            },
            None,
        )?;
        assert_eq!(
            sm.do_sync_cmd(2, Command::Get { key: "Jose".into() }, None)
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn duplicate_writes_once() -> Result<(), SummersetError> {
        let mut sm = StateMachine::new_and_setup(0).await?;
        let put = |value: &str| Command::Put {
            key: "Jose".into(),
            value: value.into(),
        };
        sm.submit_cmd(0, put("179"), Some((7, 0)))?;
        sm.submit_cmd(1, put("180"), Some((7, 1)))?;
        // a retry of request 1 gets its original result and changes nothing
        let first = CommandResult::Put {
            old_value: Some("179".into()),
        };
        let (_, result) = sm.do_sync_cmd(2, put("180"), Some((7, 1))).await?;
        assert_eq!(result, first);
        let (_, result) = sm.do_sync_cmd(3, put("181"), Some((8, 1))).await?;
        assert_eq!(
            result,
            CommandResult::Put {
                old_value: Some("180".into())
            }
        );
        // a stale duplicate of an older request does not clobber later writes
        sm.submit_cmd(4, put("179"), Some((8, 0)))?;
        let get = Command::Get { key: "Jose".into() };
        let (_, result) = sm.do_sync_cmd(5, get, None).await?;
        assert_eq!(
            result,
            CommandResult::Get {
                value: Some("181".into())
            }
        );
        // sessions survive being restored elsewhere
        let sessions = sm.get_sessions(|_| true).await?;
        let mut other = StateMachine::new_and_setup(1).await?;
        other.load_sessions(sessions)?;
        let (_, result) = other.do_sync_cmd(0, put("180"), Some((7, 1))).await?;
        assert_eq!(result, first);
        Ok(())
    }
}
