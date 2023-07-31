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

/// Action command to the logger. File cursor will be positioned at EOF after
/// every action.
#[derive(Debug, Serialize, Deserialize)]
pub enum LogAction<Ent> {
    /// Read a log entry out.
    Read { offset: usize },

    /// Write a log entry to given offset.
    Write {
        entry: Ent,
        offset: usize,
        sync: bool,
    },

    /// Append a log entry to EOF; this avoids two seeks.
    Append { entry: Ent, sync: bool },

    /// Truncate the log at given offset, keeping the head part.
    Truncate { offset: usize },

    /// Discard the log before given offset, keeping the tail part.
    Discard { offset: usize },
}

/// Action result returned by the logger.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum LogResult<Ent> {
    /// `Some(entry)` if successful, else `None`.
    Read { entry: Option<Ent> },

    /// `ok` is true if offset is valid, else false. `now_size` is the size
    /// of file after this.
    Write { offset_ok: bool, now_size: usize },

    /// `now_size` is the size of file after this.
    Append { now_size: usize },

    /// `ok` is true if truncate successful, else false. `now_size` is the
    /// size of file after this.
    Truncate { offset_ok: bool, now_size: usize },

    /// `ok` is true if discard successful, else false. `now_size` is the size
    /// of file after this.
    Discard { offset_ok: bool, now_size: usize },
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
    Ent: fmt::Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
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
        if self.logger_handle.is_some() {
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
        if self.logger_handle.is_none() {
            return logged_err!(self.me; "submit_action called before setup");
        }

        match self.tx_log {
            Some(ref tx_log) => tx_log
                .send((id, action))
                .await
                .map_err(|e| SummersetError(e.to_string()))?,
            None => {
                return logged_err!(self.me; "tx_log not created yet");
            }
        }

        Ok(())
    }

    /// Waits for the next logging result by receiving from the ack channel.
    pub async fn get_result(
        &mut self,
    ) -> Result<(LogActionId, LogResult<Ent>), SummersetError> {
        if self.logger_handle.is_none() {
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
    Ent: fmt::Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    /// Read out entry at given offset.
    async fn read_entry(
        me: ReplicaId,
        backer: &mut File,
        file_size: usize,
        offset: usize,
    ) -> Result<Option<Ent>, SummersetError> {
        if offset + 8 > file_size {
            pf_warn!(
                me;
                "read header end offset {} out of file bound {}",
                offset + 8,
                file_size
            );
            return Ok(None);
        }

        // read entry length header
        backer.seek(SeekFrom::Start(offset as u64)).await?;
        let entry_len: usize = backer.read_u64().await? as usize;
        let offset_e = offset + 8 + entry_len;
        if offset_e > file_size {
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

    /// Write given entry to given offset.
    async fn write_entry(
        me: ReplicaId,
        backer: &mut File,
        file_size: usize,
        entry: &Ent,
        offset: usize,
        sync: bool,
    ) -> Result<(bool, usize), SummersetError> {
        if offset > file_size {
            // disallow holes in log file
            pf_warn!(
                me;
                "write offset {} out of file bound {}",
                offset + 8,
                file_size
            );
            return Ok((false, file_size));
        }

        let entry_bytes = encode_to_vec(entry)?;
        let entry_len = entry_bytes.len();

        // write entry length header first
        backer.seek(SeekFrom::Start(offset as u64)).await?;
        backer.write_u64(entry_len as u64).await?;

        // then entry content
        backer.write_all(&entry_bytes[..]).await?;
        backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF

        if sync {
            backer.sync_data().await?;
        }

        let entry_end = offset + 8 + entry_len;
        let now_size = if entry_end > file_size {
            entry_end
        } else {
            file_size
        };
        Ok((true, now_size))
    }

    /// Append given entry to EOF.
    async fn append_entry(
        _me: ReplicaId,
        backer: &mut File,
        file_size: usize,
        entry: &Ent,
        sync: bool,
    ) -> Result<usize, SummersetError> {
        let entry_bytes = encode_to_vec(entry)?;
        let entry_len = entry_bytes.len();

        // write entry length header first
        backer.write_u64(entry_len as u64).await?;

        // then entry content
        backer.write_all(&entry_bytes[..]).await?;

        if sync {
            backer.sync_data().await?;
        }

        Ok(file_size + 8 + entry_len)
    }

    /// Truncate the file at given index, keeping the head part.
    async fn truncate_log(
        me: ReplicaId,
        backer: &mut File,
        file_size: usize,
        offset: usize,
    ) -> Result<(bool, usize), SummersetError> {
        if offset > file_size {
            pf_warn!(
                me;
                "truncate offset {} exceeds file end {}",
                offset,
                file_size
            );
            Ok((false, file_size))
        } else {
            backer.set_len(offset as u64).await?;
            backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF

            backer.sync_all().await?;
            Ok((true, offset))
        }
    }

    /// Discard the file before given index, keeping the tail part.
    async fn discard_log(
        me: ReplicaId,
        backer: &mut File,
        file_size: usize,
        offset: usize,
    ) -> Result<(bool, usize), SummersetError> {
        if offset > file_size {
            pf_warn!(
                me;
                "discard offset {} exceeds file end {}",
                offset,
                file_size
            );
            Ok((false, file_size))
        } else {
            let tail_size = file_size - offset;
            if tail_size > 0 {
                // due to the limited interfaces provided by `tokio::fs`, we
                // read out the tail part and write it back to offset 0 to
                // achieve the effect of discarding
                let mut tail_buf: Vec<u8> = vec![0; tail_size];
                backer.seek(SeekFrom::Start(offset as u64)).await?;
                backer.read_exact(&mut tail_buf[..]).await?;

                backer.seek(SeekFrom::Start(0)).await?;
                backer.write_all(&tail_buf[..]).await?;
            }

            backer.set_len(tail_size as u64).await?;
            backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF

            backer.sync_all().await?;
            Ok((true, tail_size))
        }
    }

    /// Carry out the given action on logger. Returns a tuple of result and
    /// file size after the action.
    async fn do_action(
        me: ReplicaId,
        backer: &mut File,
        file_size: &mut usize,
        action: LogAction<Ent>,
    ) -> Result<LogResult<Ent>, SummersetError> {
        match action {
            LogAction::Read { offset } => {
                Self::read_entry(me, backer, *file_size, offset)
                    .await
                    .map(|entry| LogResult::Read { entry })
            }
            LogAction::Write {
                entry,
                offset,
                sync,
            } => {
                Self::write_entry(me, backer, *file_size, &entry, offset, sync)
                    .await
                    .map(|(offset_ok, now_size)| {
                        *file_size = now_size;
                        LogResult::Write {
                            offset_ok,
                            now_size,
                        }
                    })
            }
            LogAction::Append { entry, sync } => {
                Self::append_entry(me, backer, *file_size, &entry, sync)
                    .await
                    .map(|now_size| {
                        *file_size = now_size;
                        LogResult::Append { now_size }
                    })
            }
            LogAction::Truncate { offset } => {
                Self::truncate_log(me, backer, *file_size, offset)
                    .await
                    .map(|(offset_ok, now_size)| {
                        *file_size = now_size;
                        LogResult::Truncate {
                            offset_ok,
                            now_size,
                        }
                    })
            }
            LogAction::Discard { offset } => {
                Self::discard_log(me, backer, *file_size, offset).await.map(
                    |(offset_ok, now_size)| {
                        *file_size = now_size;
                        LogResult::Discard {
                            offset_ok,
                            now_size,
                        }
                    },
                )
            }
        }
    }

    /// Logger thread function.
    async fn logger_thread(
        me: ReplicaId,
        mut backer_file: File,
        mut rx_log: mpsc::Receiver<(LogActionId, LogAction<Ent>)>,
        tx_ack: mpsc::Sender<(LogActionId, LogResult<Ent>)>,
    ) {
        pf_debug!(me; "logger thread spawned");

        // maintain file size
        let metadata = backer_file.metadata().await;
        if let Err(e) = metadata {
            pf_error!(me; "error reading backer file metadata: {}, exitting", e);
            return;
        }
        let mut file_size: usize = metadata.unwrap().len() as usize;

        while let Some((id, action)) = rx_log.recv().await {
            pf_trace!(me; "log action {:?}", action);
            let res =
                Self::do_action(me, &mut backer_file, &mut file_size, action)
                    .await;
            if let Err(e) = res {
                pf_error!(me; "error during logging: {}", e);
                continue;
            }

            if let Err(e) = tx_ack.send((id, res.unwrap())).await {
                pf_error!(me; "error sending to tx_ack: {}", e);
            }
        }

        // channel gets closed and no messages remain
        pf_debug!(me; "logger thread exitted");
    }
}

#[cfg(test)]
mod storage_tests {
    use super::*;
    use rmp_serde::encode::to_vec as encode_to_vec;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn write_entries() -> Result<(), SummersetError> {
        let mut backer_file =
            prepare_test_file("/tmp/test-backer-0.log").await?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let (offset_ok, now_size) =
            StorageHub::write_entry(0, &mut backer_file, 0, &entry, 0, false)
                .await?;
        assert!(offset_ok);
        let (offset_ok, now_size) = StorageHub::write_entry(
            0,
            &mut backer_file,
            now_size,
            &entry,
            now_size,
            false,
        )
        .await?;
        assert!(offset_ok);
        let (offset_ok, now_size) = StorageHub::write_entry(
            0,
            &mut backer_file,
            now_size,
            &entry,
            0,
            true,
        )
        .await?;
        assert!(offset_ok);
        let (offset_ok, _) = StorageHub::write_entry(
            0,
            &mut backer_file,
            now_size,
            &entry,
            now_size + 10,
            false,
        )
        .await?;
        assert!(!offset_ok);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn append_entries() -> Result<(), SummersetError> {
        let mut backer_file =
            prepare_test_file("/tmp/test-backer-1.log").await?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let entry_bytes = encode_to_vec(&entry)?;
        let mid_size =
            StorageHub::append_entry(0, &mut backer_file, 0, &entry, false)
                .await?;
        assert!(mid_size >= entry_bytes.len());
        let end_size = StorageHub::append_entry(
            0,
            &mut backer_file,
            mid_size,
            &entry,
            true,
        )
        .await?;
        assert!(end_size - mid_size >= entry_bytes.len());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn read_entries() -> Result<(), SummersetError> {
        let mut backer_file =
            prepare_test_file("/tmp/test-backer-2.log").await?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let now_size =
            StorageHub::append_entry(0, &mut backer_file, 0, &entry, false)
                .await?;
        let now_size = StorageHub::append_entry(
            0,
            &mut backer_file,
            now_size,
            &entry,
            true,
        )
        .await?;
        assert_eq!(
            StorageHub::read_entry(0, &mut backer_file, now_size, 0).await?,
            Some(TestEntry("test-entry-dummy-string".into()))
        );
        assert_eq!(
            StorageHub::<TestEntry>::read_entry(
                0,
                &mut backer_file,
                now_size,
                now_size + 10
            )
            .await?,
            None
        );
        assert_eq!(
            StorageHub::<TestEntry>::read_entry(
                0,
                &mut backer_file,
                now_size,
                now_size - 4
            )
            .await?,
            None
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn truncate_log() -> Result<(), SummersetError> {
        let mut backer_file =
            prepare_test_file("/tmp/test-backer-3.log").await?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let mid_offset =
            StorageHub::append_entry(0, &mut backer_file, 0, &entry, false)
                .await?;
        let end_offset = StorageHub::append_entry(
            0,
            &mut backer_file,
            mid_offset,
            &entry,
            true,
        )
        .await?;
        assert_eq!(
            StorageHub::<TestEntry>::truncate_log(
                0,
                &mut backer_file,
                end_offset,
                mid_offset
            )
            .await?,
            (true, mid_offset)
        );
        assert_eq!(
            StorageHub::<TestEntry>::truncate_log(
                0,
                &mut backer_file,
                mid_offset,
                end_offset
            )
            .await?,
            (false, mid_offset)
        );
        assert_eq!(
            StorageHub::<TestEntry>::truncate_log(
                0,
                &mut backer_file,
                mid_offset,
                0
            )
            .await?,
            (true, 0)
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn discard_log() -> Result<(), SummersetError> {
        let mut backer_file =
            prepare_test_file("/tmp/test-backer-4.log").await?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let mid_offset =
            StorageHub::append_entry(0, &mut backer_file, 0, &entry, false)
                .await?;
        let end_offset = StorageHub::append_entry(
            0,
            &mut backer_file,
            mid_offset,
            &entry,
            true,
        )
        .await?;
        let tail_size = end_offset - mid_offset;
        assert_eq!(
            StorageHub::<TestEntry>::discard_log(
                0,
                &mut backer_file,
                end_offset,
                mid_offset
            )
            .await?,
            (true, tail_size)
        );
        assert_eq!(
            StorageHub::<TestEntry>::discard_log(
                0,
                &mut backer_file,
                tail_size,
                end_offset
            )
            .await?,
            (false, tail_size)
        );
        assert_eq!(
            StorageHub::<TestEntry>::discard_log(
                0,
                &mut backer_file,
                tail_size,
                tail_size
            )
            .await?,
            (true, 0)
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn hub_setup() -> Result<(), SummersetError> {
        let mut hub: StorageHub<TestEntry> = StorageHub::new(0);
        let path = Path::new("/tmp/test-backer-5.log");
        assert!(hub.setup(path, 0, 0).await.is_err());
        hub.setup(path, 100, 100).await?;
        assert!(hub.tx_log.is_some());
        assert!(hub.rx_ack.is_some());
        assert!(hub.logger_handle.is_some());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_log_ack() -> Result<(), SummersetError> {
        let mut hub = StorageHub::new(0);
        let path = Path::new("/tmp/test-backer-6.log");
        let entry = TestEntry("abcdefgh".into());
        let entry_bytes = encode_to_vec(&entry)?;
        hub.setup(path, 3, 3).await?;
        hub.submit_action(0, LogAction::Append { entry, sync: true })
            .await?;
        hub.submit_action(1, LogAction::Read { offset: 0 }).await?;
        hub.submit_action(2, LogAction::Truncate { offset: 0 })
            .await?;
        assert_eq!(
            hub.get_result().await?,
            (
                0,
                LogResult::Append {
                    now_size: 8 + entry_bytes.len()
                }
            )
        );
        assert_eq!(
            hub.get_result().await?,
            (
                1,
                LogResult::Read {
                    entry: Some(TestEntry("abcdefgh".into()))
                }
            )
        );
        assert_eq!(
            hub.get_result().await?,
            (
                2,
                LogResult::Truncate {
                    offset_ok: true,
                    now_size: 0
                }
            )
        );
        Ok(())
    }
}
