//! Summerset server durable storage logging module implementation.

use std::fmt;
use std::path::Path;
use std::io::SeekFrom;

use crate::utils::SummersetError;
use crate::server::ReplicaId;

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Log action ID type.
pub type LogActionId = u64;

/// Action command to the logger.
#[derive(Debug, Serialize, Deserialize)]
pub enum LogAction<Ent> {
    /// Read a log entry out.
    Read { offset: usize },

    /// Append a log entry.
    Append { entry: Ent, offset: usize },

    /// Truncate the log at and after given index.
    Truncate { offset: usize },
}

/// Action result returned by the logger.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum LogResult<Ent> {
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
pub struct StorageHub<Ent> {
    /// My replica ID.
    me: ReplicaId,

    /// Sender side of the log channel.
    tx_log: Option<mpsc::Sender<(LogActionId, LogAction<Ent>)>>,

    /// Receiver side of the ack channel.
    rx_ack: Option<mpsc::Receiver<(LogActionId, LogResult<Ent>)>>,

    /// Join handle of the logger thread.
    logger_handle: Option<JoinHandle<()>>,
}

// StorageHub public API implementation
impl<Ent> StorageHub<Ent>
where
    Ent: fmt::Debug + Serialize + DeserializeOwned + Send + Sync,
{
    /// Creates a new durable storage logging hub.
    pub fn new(me: ReplicaId) -> Self {
        StorageHub {
            me,
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
            return logged_err!(self.me; "setup already done");
        }
        if chan_log_cap == 0 {
            return logged_err!(
                self.me;
                "invalid chan_log_cap {}",
                chan_log_cap
            );
        }
        if chan_ack_cap == 0 {
            return logged_err!(
                self.me;
                "invalid chan_ack_cap {}",
                chan_ack_cap
            );
        }

        // prepare backing file
        if !fs::try_exists(path).await? {
            File::create(path).await?;
            pf_info!(self.me; "created backer file '{}'", path.display());
        } else {
            pf_info!(self.me; "backer file '{}' already exists", path.display());
        }
        let mut backer_file =
            OpenOptions::new().read(true).write(true).open(path).await?;
        backer_file.seek(SeekFrom::End(0)).await?; // seek to EOF

        let (tx_log, rx_log) = mpsc::channel(chan_log_cap);
        let (tx_ack, rx_ack) = mpsc::channel(chan_ack_cap);
        self.tx_log = Some(tx_log);
        self.rx_ack = Some(rx_ack);

        let logger_handle = tokio::spawn(Self::logger_thread(
            self.me,
            backer_file,
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
        if let None = self.logger_handle {
            return logged_err!(self.me; "submit_action called before setup");
        }

        match self.tx_log {
            Some(ref tx_log) => Ok(tx_log
                .send((id, action))
                .await
                .map_err(|e| SummersetError(e.to_string()))?),
            None => logged_err!(self.me; "tx_log not created yet"),
        }
    }

    /// Waits for the next logging result by receiving from the ack channel.
    pub async fn get_result(
        &mut self,
    ) -> Result<(LogActionId, LogResult<Ent>), SummersetError> {
        if let None = self.logger_handle {
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

// StorageHub logger thread implementation
impl<Ent> StorageHub<Ent>
where
    Ent: fmt::Debug + Serialize + DeserializeOwned + Send + Sync,
{
    /// Read out entry at given offset.
    /// TODO: better management of file cursor.
    /// TODO: maybe just support scanning.
    async fn read_entry(
        me: ReplicaId,
        backer: &mut File,
        offset: usize,
    ) -> Result<Option<Ent>, SummersetError> {
        let file_len: usize = backer.metadata().await?.len() as usize;
        if offset + 8 > file_len {
            pf_warn!(
                me;
                "read header end offset {} out of file bound {}",
                offset + 8,
                file_len
            );
            return Ok(None);
        }

        // read entry length header
        backer.seek(SeekFrom::Start(offset as u64)).await?;
        let entry_len: usize = backer.read_u64().await? as usize;
        let offset_e = offset + 8 + entry_len;
        if offset_e > file_len {
            pf_warn!(me; "read entry invalid length {}", entry_len);
            backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF
            return Ok(None);
        }

        // read entry content
        let mut entry_buf: Vec<u8> = vec![0; entry_len];
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
        let file_len: usize = backer.metadata().await?.len() as usize;
        if offset != file_len {
            pf_warn!(
                me;
                "append offset {} not at file end {}",
                offset,
                file_len
            );
            Ok((false, file_len))
        } else {
            let entry_bytes = encode_to_vec(entry)?;
            // write entry length header first
            let entry_len = entry_bytes.len();
            backer.write_u64(entry_len as u64).await?;
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
        let file_len: usize = backer.metadata().await?.len() as usize;
        if offset > file_len {
            pf_warn!(
                me;
                "truncate offset {} exceeds file end {}",
                offset,
                file_len
            );
            Ok((false, file_len))
        } else {
            backer.set_len(offset as u64).await?;
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
                    |(ok, now_offset)| LogResult::TruncateResult {
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
        mut backer: File,
        mut rx_log: mpsc::Receiver<(LogActionId, LogAction<Ent>)>,
        tx_ack: mpsc::Sender<(LogActionId, LogResult<Ent>)>,
    ) {
        pf_debug!(me; "logger thread spawned");

        loop {
            match rx_log.recv().await {
                Some((id, action)) => {
                    pf_trace!(me; "log action {:?}", action);
                    let res = Self::do_action(me, &mut backer, action).await;
                    if let Err(e) = res {
                        pf_error!(me; "error during logging: {}", e);
                        continue;
                    }

                    if let Err(e) = tx_ack.send((id, res.unwrap())).await {
                        pf_error!(me; "error sending to tx_ack: {}", e);
                    }
                }

                None => break, // channel gets closed and no messages remain
            }
        }

        pf_debug!(me; "logger thread exitted");
    }
}

#[cfg(test)]
mod storage_tests {
    use super::*;
    use rmp_serde::encode::to_vec as encode_to_vec;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEntry(String);

    async fn prepare_test_file(path: &str) -> Result<File, SummersetError> {
        if !fs::try_exists(path).await? {
            File::create(path).await?;
        } else {
            let file = OpenOptions::new().write(true).open(path).await?;
            file.set_len(0).await?;
        }
        let file = OpenOptions::new().read(true).write(true).open(path).await?;
        Ok(file)
    }

    #[test]
    fn append_entries() -> Result<(), SummersetError> {
        let mut backer_file =
            tokio_test::block_on(prepare_test_file("/tmp/test-backer-0.log"))?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_file,
            &entry,
            0,
        ))?;
        assert!(ok);
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_file,
            &entry,
            now_offset,
        ))?;
        assert!(ok);
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_file,
            &entry,
            now_offset + 10,
        ))?;
        assert!(!ok);
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_file,
            &entry,
            0,
        ))?;
        assert!(!ok);
        Ok(())
    }

    #[test]
    fn read_entries() -> Result<(), SummersetError> {
        let mut backer_file =
            tokio_test::block_on(prepare_test_file("/tmp/test-backer-1.log"))?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_file,
            &entry,
            0,
        ))?;
        assert!(ok);
        let (ok, now_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_file,
            &entry,
            now_offset,
        ))?;
        assert!(ok);
        assert_eq!(
            tokio_test::block_on(StorageHub::read_entry(
                0,
                &mut backer_file,
                0
            ))?,
            Some(TestEntry("test-entry-dummy-string".into()))
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::<TestEntry>::read_entry(
                0,
                &mut backer_file,
                now_offset + 10
            ))?,
            None
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::<TestEntry>::read_entry(
                0,
                &mut backer_file,
                now_offset - 4
            ))?,
            None
        );
        Ok(())
    }

    #[test]
    fn truncate_log() -> Result<(), SummersetError> {
        let mut backer_file =
            tokio_test::block_on(prepare_test_file("/tmp/test-backer-2.log"))?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let (ok, mid_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_file,
            &entry,
            0,
        ))?;
        assert!(ok);
        let (ok, end_offset) = tokio_test::block_on(StorageHub::append_entry(
            0,
            &mut backer_file,
            &entry,
            mid_offset,
        ))?;
        assert!(ok);
        assert_eq!(
            tokio_test::block_on(StorageHub::<TestEntry>::truncate_log(
                0,
                &mut backer_file,
                mid_offset
            ))?,
            (true, mid_offset)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::<TestEntry>::truncate_log(
                0,
                &mut backer_file,
                end_offset
            ))?,
            (false, mid_offset)
        );
        assert_eq!(
            tokio_test::block_on(StorageHub::<TestEntry>::truncate_log(
                0,
                &mut backer_file,
                0
            ))?,
            (true, 0)
        );
        Ok(())
    }

    #[test]
    fn hub_setup() -> Result<(), SummersetError> {
        let mut hub: StorageHub<TestEntry> = StorageHub::new(0);
        let path = Path::new("/tmp/test-backer-3.log");
        assert!(tokio_test::block_on(hub.setup(&path, 0, 0)).is_err());
        tokio_test::block_on(hub.setup(&path, 100, 100))?;
        assert!(hub.tx_log.is_some());
        assert!(hub.rx_ack.is_some());
        assert!(hub.logger_handle.is_some());
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
