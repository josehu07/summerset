//! Summerset server durable storage logging module implementation.

use std::path::Path;
use std::io::SeekFrom;
use std::mem::size_of;
use std::sync::Arc;

use crate::core::utils::{SummersetError, ReplicaId};
use crate::core::replica::GeneralReplica;

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::fs::{self, File, OpenOptions};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use log::{trace, debug, info, warn, error};

/// Action command to the logger.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum LogAction<Ent>
where
    Ent: PartialEq + Eq + Clone + Serialize + DeserializeOwned,
{
    /// Read a log entry out.
    Read { idx: usize },

    /// Append a log entry.
    Append { entry: Ent, idx: usize },

    /// Truncate the log at and after given index.
    Truncate { idx: usize },
}

/// Action result returned by the logger.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum LogResult<Ent>
where
    Ent: PartialEq + Eq + Clone + Serialize + DeserializeOwned,
{
    /// `Some(entry)` if successful, else `None`.
    ReadResult { entry: Option<Ent> },

    /// `ok` is true if append successful, else false. `idx` is the index of
    /// the next empty slot in log after this append.
    AppendResult { ok: bool, idx: usize },

    /// `ok` is true if truncate successful, else false. `idx` is the index of
    /// the next empty slot in log after this truncate.
    TruncateResult { ok: bool, idx: usize },
}

/// Durable storage logging module.
#[derive(Debug)]
pub struct StorageHub<'r, Rpl, Ent>
where
    Rpl: 'r + GeneralReplica,
    Ent: PartialEq + Eq + Clone + Serialize + DeserializeOwned,
{
    /// Reference to protocol-specific replica struct.
    replica: &'r Rpl,

    /// Backing file for durability.
    backer: Option<Arc<Mutex<File>>>,

    /// Sender side of the log channel.
    tx_log: Option<mpsc::Sender<LogAction<Ent>>>,

    /// Receiver side of the ack channel.
    rx_ack: Option<mpsc::Receiver<LogResult<Ent>>>,

    /// Join handle of the logger thread.
    logger_handle: Option<JoinHandle<()>>,
}

// StorageHub public API implementation
impl<'r, Rpl, Ent> StorageHub<'r, Rpl, Ent> {
    /// Size of a log entry in bytes.
    const ENTRY_SIZE: usize = size_of::<Ent>();

    /// Creates a new durable storage logging hub.
    pub fn new(replica: &'r Rpl) -> Self {
        StorageHub {
            replica,
            backer: None,
            tx_log: None,
            rx_ack: None,
            logger_handle: None,
        }
    }

    /// Spawns the logger thread. Creates a log channel for submitting logging
    /// actions to the logger and an ack channel for getting results. Prepares
    /// the given backing file as durability backend.
    pub async fn setup(
        &mut self,
        path: &Path,
        chan_log_cap: usize,
        chan_ack_cap: usize,
    ) -> Result<(), SummersetError> {
        let me = self.replica.id();

        if let Some(_) = self.logger_handle {
            return logged_err!(me, "logger thread already spawned");
        }
        if chan_log_cap == 0 {
            return logged_err!(me, "invalid chan_log_cap {}", chan_log_cap);
        }
        if chan_ack_cap == 0 {
            return logged_err!(me, "invalid chan_ack_cap {}", chan_ack_cap);
        }

        // prepare backing file
        if !fs::try_exists(path).await? {
            File::create(path).await?;
            pf_info!(me, "created backer file '{}'", path);
        } else {
            let mut file = OpenOptions::new().write(true).open(path).await?;
            file.set_len(0).await?;
            pf_info!(me, "truncated backer file '{}'", path);
        }
        let mut file =
            OpenOptions::new().read(true).write(true).open(path).await?;
        self.backer = Some(Arc::new(Mutex::new(file)));

        let (tx_log, mut rx_log) = mpsc::channel(chan_log_cap);
        let (tx_ack, mut rx_ack) = mpsc::channel(chan_ack_cap);
        self.tx_log = Some(tx_log);
        self.rx_ack = Some(rx_ack);

        let logger_handle = tokio::spawn(Self::logger_thread(
            me,
            self.backer.unwrap().clone(),
            rx_log,
            tx_ack,
        ));
        self.logger_handle = Some(logger_handle);

        Ok(())
    }

    /// Submits an action by sending it to the log channel.
    pub async fn submit_action(
        &mut self,
        action: LogAction<Ent>,
    ) -> Result<(), SummersetError> {
        if let None = self.backer {
            return logged_err!(
                self.replica.id(),
                "submit_action called before setup"
            );
        }

        match self.tx_log {
            Some(ref tx_log) => Ok(tx_log.send(action).await?),
            None => logged_err!(self.replica.id(), "tx_log not created yet"),
        }
    }

    /// Waits for the next logging result by receiving from the ack channel.
    pub async fn get_result(
        &mut self,
    ) -> Result<LogResult<Ent>, SummersetError> {
        if let None = self.backer {
            return logged_err!(
                self.replica.id(),
                "get_result called before setup"
            );
        }

        match self.rx_ack {
            Some(ref mut rx_ack) => match rx_ack.recv().await {
                Some(result) => Ok(result),
                None => logged_err!(
                    self.replica.id(),
                    "ack channel has been closed"
                ),
            },
            None => logged_err!(self.replica.id(), "rx_ack not created yet"),
        }
    }
}

// StorageHub logger thread implementation
impl<'r, Rpl, Ent> StorageHub<'r, Rpl, Ent> {
    /// Compute file offset from entry index.
    fn idx_to_offset(idx: usize) -> Result<usize, SummersetError> {
        Ok(Self::ENTRY_SIZE * idx)
    }

    /// Compute entry index from file offset.
    fn offset_to_idx(offset: usize) -> Result<usize, SummersetError> {
        if offset % Self::ENTRY_SIZE != 0 {
            Err(SummersetError(format!(
                "invalid offset {} given entry size {}",
                offset,
                Self::ENTRY_SIZE
            )))
        } else {
            Ok(offset / Self::ENTRY_SIZE)
        }
    }

    /// Read out entry at given index.
    /// TODO: better management of file cursor.
    async fn read_entry(
        me: ReplicaId,
        backer: &mut File,
        idx: usize,
    ) -> Result<Option<Ent>, SummersetError> {
        let file_len = backer.metadata().await?.len();
        let offset_s = Self::idx_to_offset(idx)?;
        let offset_e = Self::idx_to_offset(idx + 1)?;

        if offset_e > file_len {
            pf_warn!(
                me,
                "read idx {} out of file bound {} / {}",
                idx,
                offset_e,
                file_len
            );
            Ok(None)
        } else {
            backer.seek(SeekFrom::Start(offset_s)).await?;
            let entry_buf: Vec<u8> = vec![0; Self::ENTRY_SIZE];
            backer.read_exact(&mut entry_buf[..]).await?;
            let entry = decode_from_slice(&entry_buf)?;
            backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF
            Ok(Some(entry))
        }
    }

    /// Append given entry to given index.
    async fn append_entry(
        me: ReplicaId,
        backer: &mut File,
        entry: &Ent,
        idx: usize,
    ) -> Result<(bool, usize), SummersetError> {
        let file_len = backer.metadata().await?.len();
        let offset = Self::idx_to_offset(idx)?;

        if offset != file_len {
            pf_warn!(
                me,
                "append idx {} not at file end {} / {}",
                idx,
                offset,
                file_len
            );
            Ok((false, Self::offset_to_idx(file_len)?))
        } else {
            let entry_bytes = encode_to_vec(entry)?;
            backer.write_all(&entry_bytes[..]).await?;
            Ok((true, idx + 1))
        }
    }

    /// Truncate the file to given index.
    async fn truncate_log(
        me: ReplicaId,
        backer: &mut File,
        idx: usize,
    ) -> Result<(bool, usize), SummersetError> {
        let file_len = backer.metadata().await?.len();
        let offset = Self::idx_to_offset(idx)?;

        if offset > file_len {
            pf_warn!(
                me,
                "truncate idx {} exceeds file end {} / {}",
                idx,
                offset,
                file_len
            );
            Ok((false, Self::offset_to_idx(file_len)?))
        } else {
            backer.set_len(offset).await?;
            Ok((true, idx))
        }
    }

    /// Carry out the given action on logger.
    async fn do_action(
        me: ReplicaId,
        backer: &mut File,
        action: LogAction<Ent>,
    ) -> Result<LogResult<Ent>, SummersetError> {
        let result = match action {
            LogAction::Read { idx } => Self::read_entry(me, backer, idx)
                .await
                .map(|entry| LogResult::ReadResult { entry }),
            LogAction::Append { entry, idx } => {
                Self::append_entry(me, backer, &entry, idx)
                    .await
                    .map(|(ok, cidx)| LogResult::AppendResult { ok, idx: cidx })
            }
            LogAction::Truncate { idx } => Self::truncate_log(me, backer, idx)
                .await
                .map(|ok, cidx| LogResult::TruncateResult { ok, idx: cidx }),
        };

        result
    }

    /// Logger thread function.
    async fn logger_thread(
        me: ReplicaId,
        backer: Arc<Mutex<File>>,
        mut rx_log: mpsc::Receiver<LogAction<Ent>>,
        tx_ack: mpsc::Sender<LogResult<Ent>>,
    ) {
        pf_debug!(me, "logger thread spawned");

        loop {
            match rx_log.recv().await {
                Some(action) => {
                    pf_trace!(me, "log action {:?}", action);
                    let res = {
                        // need tokio::sync::Mutex here since held across await
                        let mut backer_guard = backer.lock().unwrap();
                        Self::do_action(me, &mut backer_guard, action).await
                    };
                    if let Err(e) = res {
                        pf_error!(me, "error during logging: {}", e);
                        continue;
                    }

                    if let Err(e) = tx_ack.send(res.unwrap()).await {
                        pf_error!(me, "error sending to tx_ack: {}", e);
                    }
                }

                None => break, // channel gets closed and no messages remain
            }
        }

        pf_debug!(me, "logger thread exitted");
    }
}

#[cfg(test)]
mod storage_tests {
    use super::*;
    use crate::core::replica::DummyReplica;

    #[derive(PartialEq, Eq, Clone, Serialize, Deserialize)]
    struct TestEntry(String);

    #[test]
    fn hub_setup() -> Result<(), SummersetError> {
        let replica = DummyReplica::new(0, 3, "127.0.0.1:52800".into());
        let mut hub = StorageHub::new(&replica);
        let path = Path::new("test-backer.log");
        assert!(tokio_test::block_on(hub.setup(&path, 0, 0)).is_err());
        tokio_test::block_on(hub.setup(&path, 100, 100))?;
        assert!(hub.backer.is_some());
        assert!(hub.tx_log.is_some());
        assert!(hub.rx_ack.is_some());
        assert!(hub.logger_handle.is_some());
        Ok(())
    }

    #[test]
    fn append_entries() -> Result<(), SummersetError> {
        let replica = DummyReplica::new(0, 3, "127.0.0.1:52800".into());
        let mut hub = StorageHub::new(&replica);
        let path = Path::new("test-backer.log");
        tokio_test::block_on(hub.setup(&path, 1, 1))?;
        let mut backer_guard = hub.backer.unwrap().lock().unwrap();
        let entry = TestEntry("test-entry-dummy-string".into());
        assert_eq!(
            tokio_test::block_on(StorageHub::append_entry(
                replica.id(),
                &mut backer_guard,
                &entry,
                0
            ))?,
            (true, 1)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::append_entry(
                replica.id(),
                &mut backer_guard,
                &entry,
                1
            ))?,
            (true, 2)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::append_entry(
                replica.id(),
                &mut backer_guard,
                &entry,
                1
            ))?,
            (false, 2)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::append_entry(
                replica.id(),
                &mut backer_guard,
                &entry,
                3
            ))?,
            (false, 2)
        );
        Ok(())
    }

    #[test]
    fn read_entries() -> Result<(), SummersetError> {
        let replica = DummyReplica::new(0, 3, "127.0.0.1:52800".into());
        let mut hub = StorageHub::new(&replica);
        let path = Path::new("test-backer.log");
        tokio_test::block_on(hub.setup(&path, 1, 1))?;
        let mut backer_guard = hub.backer.unwrap().lock().unwrap();
        let entry = TestEntry("test-entry-dummy-string".into());
        assert_eq!(
            tokio_test::block_on(StorageHub::append_entry(
                replica.id(),
                &mut backer_guard,
                &entry,
                0
            ))?,
            (true, 1)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::append_entry(
                replica.id(),
                &mut backer_guard,
                &entry,
                1
            ))?,
            (true, 2)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::read_entry(
                replica.id(),
                &mut backer_guard,
                0
            ))?,
            Some("test-entry-dummy-string".into())
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::read_entry(
                replica.id(),
                &mut backer_guard,
                3
            ))?,
            None
        );
        Ok(())
    }

    #[test]
    fn truncate_log() -> Result<(), SummersetError> {
        let replica = DummyReplica::new(0, 3, "127.0.0.1:52800".into());
        let mut hub = StorageHub::new(&replica);
        let path = Path::new("test-backer.log");
        tokio_test::block_on(hub.setup(&path, 1, 1))?;
        let mut backer_guard = hub.backer.unwrap().lock().unwrap();
        let entry = TestEntry("test-entry-dummy-string".into());
        assert_eq!(
            tokio_test::block_on(StorageHub::append_entry(
                replica.id(),
                &mut backer_guard,
                &entry,
                0
            ))?,
            (true, 1)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::append_entry(
                replica.id(),
                &mut backer_guard,
                &entry,
                1
            ))?,
            (true, 2)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::truncate_log(
                replica.id(),
                &mut backer_guard,
                1
            ))?,
            (true, 1)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::truncate_log(
                replica.id(),
                &mut backer_guard,
                3
            ))?,
            (false, 1)
        );
        Ok(())
    }

    #[test]
    fn log_ack_api() -> Result<(), SummersetError> {
        let replica = DummyReplica::new(0, 3, "127.0.0.1:52800".into());
        let mut hub = StorageHub::new(&replica);
        let path = Path::new("test-backer.log");
        let entry = TestEntry("abcdefgh".into());
        tokio_test::block_on(hub.setup(&path, 3, 3))?;
        tokio_test::block_on(
            hub.submit_action(LogAction::Append { entry, idx: 0 }),
        )?;
        tokio_test::block_on(hub.submit_action(LogAction::Read { idx: 0 }))?;
        tokio_test::block_on(
            hub.submit_action(LogAction::Truncate { idx: 0 }),
        )?;
        assert_eq!(
            tokio_test::block_on(hub.get_result())?,
            LogResult::AppendResult { ok: true, idx: 1 }
        );
        assert_eq!(
            tokio_test::block_on(hub.get_result())?,
            LogResult::ReadResult {
                entry: Some(TestEntry("abcdefgh".into()))
            }
        );
        assert_eq!(
            tokio_test::block_on(hub.get_result())?,
            LogResult::TruncateResult { ok: true, idx: 0 }
        );
        Ok(())
    }
}
