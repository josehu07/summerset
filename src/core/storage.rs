//! Summerset server durable storage logging module implementation.

use std::path::Path;
use std::io::SeekFrom;
use std::sync::Arc;

use crate::core::utils::SummersetError;
use crate::core::replica::ReplicaId;

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::fs::{self, File, OpenOptions};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use log::{trace, debug, info, warn, error};

/// Log action ID type.
pub type LogActionId = u64;

/// Action command to the logger.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum LogAction<Ent>
where
    Ent: PartialEq + Eq + Clone + Serialize + DeserializeOwned,
{
    /// Read a log entry out.
    Read { offset: usize },

    /// Append a log entry.
    Append { entry: Ent, offset: usize },

    /// Truncate the log at and after given index.
    Truncate { offset: usize },
}

/// Action result returned by the logger.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum LogResult<Ent>
where
    Ent: PartialEq + Eq + Clone + Serialize + DeserializeOwned,
{
    /// `Some(entry)` if successful, else `None`.
    ReadResult { entry: Option<Ent> },

    /// `ok` is true if append successful, else false. `offset` is the offset
    /// of the file cursor after this.
    AppendResult { ok: bool, offset: usize },

    /// `ok` is true if truncate successful, else false. `offset` is the
    /// offset of the file cursor after this.
    TruncateResult { ok: bool, offset: usize },
}

/// Durable storage logging module.
#[derive(Debug)]
pub struct StorageHub<Ent>
where
    Ent: PartialEq + Eq + Clone + Serialize + DeserializeOwned,
{
    /// My replica ID.
    me: ReplicaId,

    /// Backing file for durability.
    backer: Option<Arc<Mutex<File>>>,

    /// Sender side of the log channel.
    tx_log: Option<mpsc::Sender<(LogActionId, LogAction<Ent>)>>,

    /// Receiver side of the ack channel.
    rx_ack: Option<mpsc::Receiver<(LogActionId, LogResult<Ent>)>>,

    /// Join handle of the logger thread.
    logger_handle: Option<JoinHandle<()>>,
}

// StorageHub public API implementation
impl<Ent> StorageHub<Ent> {
    /// Creates a new durable storage logging hub.
    pub fn new(me: ReplicaId) -> Self {
        StorageHub {
            me,
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
        if let Some(_) = self.logger_handle {
            return logged_err!(self.me, "logger thread already spawned");
        }
        if chan_log_cap == 0 {
            return logged_err!(
                self.me,
                "invalid chan_log_cap {}",
                chan_log_cap
            );
        }
        if chan_ack_cap == 0 {
            return logged_err!(
                self.me,
                "invalid chan_ack_cap {}",
                chan_ack_cap
            );
        }

        // prepare backing file
        if !fs::try_exists(path).await? {
            File::create(path).await?;
            pf_info!(self.me, "created backer file '{}'", path);
        } else {
            pf_info!(self.me, "backer file '{}' already exists", path);
        }
        let mut file =
            OpenOptions::new().read(true).write(true).open(path).await?;
        file.seek(SeekFrom::End(0)).await?; // seek to EOF
        self.backer = Some(Arc::new(Mutex::new(file)));

        let (tx_log, mut rx_log) = mpsc::channel(chan_log_cap);
        let (tx_ack, mut rx_ack) = mpsc::channel(chan_ack_cap);
        self.tx_log = Some(tx_log);
        self.rx_ack = Some(rx_ack);

        let logger_handle = tokio::spawn(Self::logger_thread(
            self.me,
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
        id: LogActionId,
        action: LogAction<Ent>,
    ) -> Result<(), SummersetError> {
        if let None = self.backer {
            return logged_err!(self.me, "submit_action called before setup");
        }

        match self.tx_log {
            Some(ref tx_log) => Ok(tx_log.send((id, action)).await?),
            None => logged_err!(self.me, "tx_log not created yet"),
        }
    }

    /// Waits for the next logging result by receiving from the ack channel.
    pub async fn get_result(
        &mut self,
    ) -> Result<LogResult<Ent>, SummersetError> {
        if let None = self.backer {
            return logged_err!(self.me, "get_result called before setup");
        }

        match self.rx_ack {
            Some(ref mut rx_ack) => match rx_ack.recv().await {
                Some((id, result)) => Ok((id, result)),
                None => logged_err!(self.me, "ack channel has been closed"),
            },
            None => logged_err!(self.me, "rx_ack not created yet"),
        }
    }
}

// StorageHub logger thread implementation
impl<Ent> StorageHub<Ent> {
    /// Read out entry at given offset.
    /// TODO: better management of file cursor.
    /// TODO: maybe just support scanning.
    async fn read_entry(
        me: ReplicaId,
        backer: &mut File,
        offset: usize,
    ) -> Result<Option<Ent>, SummersetError> {
        let file_len = backer.metadata().await?.len();
        if offset + 8 > file_len {
            pf_warn!(
                me,
                "read header end offset {} out of file bound {}",
                offset + 8,
                file_len
            );
            return Ok(None);
        }

        // read entry length header
        backer.seek(SeekFrom::Start(offset)).await?;
        let entry_len = backer.read_u64().await?;
        let offset_e = offset + 8 + entry_len;
        if offset_e > file_len {
            pf_warn!(me, "read entry invalid length {}", entry_len);
            backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF
            return Ok(None);
        }

        // read entry content
        let entry_buf: Vec<u8> = vec![0; entry_len];
        backer.read_exact(&mut entry_buf[..]).await?;
        let entry = decode_from_slice(&entry_buf)?;
        backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF
        Ok(Some(entry))
    }

    /// Append given entry to given offset, which must be current EOF.
    async fn append_entry(
        me: ReplicaId,
        backer: &mut File,
        entry: &Ent,
        offset: usize,
    ) -> Result<(bool, usize), SummersetError> {
        let file_len = backer.metadata().await?.len();
        if offset != file_len {
            pf_warn!(
                me,
                "append offset {} not at file end {}",
                offset,
                file_len
            );
            Ok((false, file_len))
        } else {
            let entry_bytes = encode_to_vec(entry)?;
            // write entry length header first
            let entry_len: u64 = entry_bytes.len();
            backer.write_u64(entry_len).await?;
            // then entry content
            backer.write_all(&entry_bytes[..]).await?;
            Ok((true, offset + 8 + entry_len))
        }
    }

    /// Truncate the file to given index.
    async fn truncate_log(
        me: ReplicaId,
        backer: &mut File,
        offset: usize,
    ) -> Result<(bool, usize), SummersetError> {
        let file_len = backer.metadata().await?.len();
        if offset > file_len {
            pf_warn!(
                me,
                "truncate offset {} exceeds file end {}",
                offset,
                file_len
            );
            Ok((false, file_len))
        } else {
            backer.set_len(offset).await?;
            Ok((true, offset))
        }
    }

    /// Carry out the given action on logger.
    async fn do_action(
        me: ReplicaId,
        backer: &mut File,
        action: LogAction<Ent>,
    ) -> Result<LogResult<Ent>, SummersetError> {
        let result = match action {
            LogAction::Read { offset } => Self::read_entry(me, backer, offset)
                .await
                .map(|entry| LogResult::ReadResult { entry }),
            LogAction::Append { entry, offset } => {
                Self::append_entry(me, backer, &entry, offset).await.map(
                    |(ok, now_offset)| LogResult::AppendResult {
                        ok,
                        offset: now_offset,
                    },
                )
            }
            LogAction::Truncate { offset } => {
                Self::truncate_log(me, backer, offset).await.map(
                    |ok, now_offset| LogResult::TruncateResult {
                        ok,
                        offset: now_offset,
                    },
                )
            }
        };

        result
    }

    /// Logger thread function.
    async fn logger_thread(
        me: ReplicaId,
        backer: Arc<Mutex<File>>,
        mut rx_log: mpsc::Receiver<(LogActionId, LogAction<Ent>)>,
        tx_ack: mpsc::Sender<(LogActionId, LogResult<Ent>)>,
    ) {
        pf_debug!(me, "logger thread spawned");

        loop {
            match rx_log.recv().await {
                Some((id, action)) => {
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

                    if let Err(e) = tx_ack.send((id, res.unwrap())).await {
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
    use rmp_serde::encode::to_vec as encode_to_vec;

    #[test]
    fn hub_setup() -> Result<(), SummersetError> {
        let mut hub = StorageHub::new(0);
        let path = Path::new("/tmp/test-backer-0.log");
        assert!(tokio_test::block_on(hub.setup(&path, 0, 0)).is_err());
        tokio_test::block_on(hub.setup(&path, 100, 100))?;
        assert!(hub.backer.is_some());
        assert!(hub.tx_log.is_some());
        assert!(hub.rx_ack.is_some());
        assert!(hub.logger_handle.is_some());
        Ok(())
    }

    #[derive(PartialEq, Eq, Clone, Serialize, Deserialize)]
    struct TestEntry(String);

    #[test]
    fn append_entries() -> Result<(), SummersetError> {
        let mut hub = StorageHub::new(0);
        let path = Path::new("/tmp/test-backer-1.log");
        tokio_test::block_on(hub.setup(&path, 1, 1))?;
        let mut backer_guard = hub.backer.unwrap().lock().unwrap();
        let entry = TestEntry("test-entry-dummy-string".into());
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_guard,
            &entry,
            0,
        ))?;
        assert!(ok);
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_guard,
            &entry,
            now_offset,
        ))?;
        assert!(ok);
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_guard,
            &entry,
            now_offset + 10,
        ))?;
        assert!(!ok);
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_guard,
            &entry,
            0,
        ))?;
        assert!(!ok);
        Ok(())
    }

    #[test]
    fn read_entries() -> Result<(), SummersetError> {
        let mut hub = StorageHub::new(0);
        let path = Path::new("/tmp/test-backer-2.log");
        tokio_test::block_on(hub.setup(&path, 1, 1))?;
        let mut backer_guard = hub.backer.unwrap().lock().unwrap();
        let entry = TestEntry("test-entry-dummy-string".into());
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_guard,
            &entry,
            0,
        ))?;
        assert!(ok);
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_guard,
            &entry,
            now_offset,
        ))?;
        assert!(ok);
        assert_eq!(
            tokio_test::block_on(StorageHub::read_entry(
                0,
                &mut backer_guard,
                0
            ))?,
            Some(TestEntry("test-entry-dummy-string".into()))
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::read_entry(
                0,
                &mut backer_guard,
                now_offset + 10
            ))?,
            None
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::read_entry(
                0,
                &mut backer_guard,
                now_offset - 4
            ))?,
            None
        );
        Ok(())
    }

    #[test]
    fn truncate_log() -> Result<(), SummersetError> {
        let mut hub = StorageHub::new(0);
        let path = Path::new("/tmp/test-backer-3.log");
        tokio_test::block_on(hub.setup(&path, 1, 1))?;
        let mut backer_guard = hub.backer.unwrap().lock().unwrap();
        let entry = TestEntry("test-entry-dummy-string".into());
        let (ok, mid_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_guard,
            &entry,
            0,
        ))?;
        assert!(ok);
        let (ok, end_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_guard,
            &entry,
            mid_offset,
        ))?;
        assert!(ok);
        assert_eq!(
            tokio_test::block_on(StorageHub::truncate_log(
                0,
                &mut backer_guard,
                mid_offset
            ))?,
            (true, mid_offset)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::truncate_log(
                0,
                &mut backer_guard,
                end_offset
            ))?,
            (false, mid_offset)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::truncate_log(
                0,
                &mut backer_guard,
                0
            ))?,
            (true, 0)
        );
        Ok(())
    }

    #[test]
    fn log_ack_api() -> Result<(), SummersetError> {
        let mut hub = StorageHub::new(0);
        let path = Path::new("/tmp/test-backer-4.log");
        let entry = TestEntry("abcdefgh".into());
        let entry_bytes = encode_to_vec(&entry)?;
        tokio_test::block_on(hub.setup(&path, 3, 3))?;
        tokio_test::block_on(
            hub.submit_action(0, LogAction::Append { entry, offset: 0 }),
        )?;
        tokio_test::block_on(
            hub.submit_action(1, LogAction::Read { offset: 0 }),
        )?;
        tokio_test::block_on(
            hub.submit_action(2, LogAction::Truncate { offset: 0 }),
        )?;
        assert_eq!(
            tokio_test::block_on(hub.get_result())?,
            (
                0,
                LogResult::AppendResult {
                    ok: true,
                    offset: 8 + entry_bytes.len()
                }
            )
        );
        assert_eq!(
            tokio_test::block_on(hub.get_result())?,
            (
                1,
                LogResult::ReadResult {
                    entry: Some(TestEntry("abcdefgh".into()))
                }
            )
        );
        assert_eq!(
            tokio_test::block_on(hub.get_result())?,
            (
                2,
                LogResult::TruncateResult {
                    ok: true,
                    offset: 0
                }
            )
        );
        Ok(())
    }
}
