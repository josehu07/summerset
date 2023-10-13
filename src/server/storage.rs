//! Summerset server durable storage logging module implementation.

use std::fmt;
use std::path::Path;
use std::io::SeekFrom;
use std::sync::Arc;

use crate::utils::SummersetError;
use crate::server::ReplicaId;

use get_size::GetSize;

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

/// Log action ID type.
pub type LogActionId = u64;

/// Action command to the logger. File cursor will be positioned at EOF after
/// every action.
#[derive(Debug, Serialize, Deserialize, GetSize)]
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

    /// Discard the log before given offset, keeping the tail part (and
    /// optionally a head part).
    Discard { offset: usize, keep: usize },
}

/// Action result returned by the logger.
#[derive(Debug, Serialize, Deserialize, PartialEq, GetSize)]
pub enum LogResult<Ent> {
    /// `Some(entry)` if successful, else `None`.
    Read {
        entry: Option<Ent>,
        end_offset: usize,
    },

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
    tx_log: mpsc::UnboundedSender<(LogActionId, LogAction<Ent>)>,

    /// Receiver side of the ack channel.
    rx_ack: mpsc::UnboundedReceiver<(LogActionId, LogResult<Ent>)>,

    /// Join handle of the logger thread.
    _logger_handle: JoinHandle<()>,
}

// StorageHub public API implementation
impl<Ent> StorageHub<Ent>
where
    Ent: fmt::Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + GetSize
        + Send
        + Sync
        + 'static,
{
    /// Creates a new durable storage logging hub. Spawns the logger thread.
    /// Creates a log channel for submitting logging actions to the logger and
    /// an ack channel for getting results. Prepares the given backing file as
    /// durability backend.
    pub async fn new_and_setup(
        me: ReplicaId,
        path: &Path,
        perf_a_b: Option<(u64, u64)>, // performance simulation params
    ) -> Result<Self, SummersetError> {
        // prepare backing file
        if !fs::try_exists(path).await? {
            File::create(path).await?;
            pf_info!(me; "created backer file '{}'", path.display());
        } else {
            pf_info!(me; "backer file '{}' already exists", path.display());
        }
        let mut backer_file =
            OpenOptions::new().read(true).write(true).open(path).await?;
        backer_file.seek(SeekFrom::End(0)).await?; // seek to EOF

        let (tx_log, mut rx_log) =
            mpsc::unbounded_channel::<(LogActionId, LogAction<Ent>)>();
        let (tx_ack, rx_ack) = mpsc::unbounded_channel();

        // if doing performance delay simulation, add on-the-fly delay to
        // each message received
        let rx_log_true = if let Some((perf_a, perf_b)) = perf_a_b {
            let (tx_log_delayed, rx_log_delayed) = mpsc::unbounded_channel();
            let tx_log_delayed_arc = Arc::new(tx_log_delayed);

            tokio::spawn(async move {
                while let Some((id, log_action)) = rx_log.recv().await {
                    let tx_log_delayed_clone = tx_log_delayed_arc.clone();
                    tokio::spawn(async move {
                        let approx_size = log_action.get_size() as u64;
                        let delay_ns = perf_a + approx_size * perf_b;
                        time::sleep(Duration::from_nanos(delay_ns)).await;
                        tx_log_delayed_clone.send((id, log_action)).unwrap();
                    });
                }
            });

            rx_log_delayed
        } else {
            rx_log
        };

        let logger_handle = tokio::spawn(Self::logger_thread(
            me,
            backer_file,
            rx_log_true,
            tx_ack,
        ));

        Ok(StorageHub {
            me,
            tx_log,
            rx_ack,
            _logger_handle: logger_handle,
        })
    }

    /// Submits an action by sending it to the log channel.
    pub fn submit_action(
        &mut self,
        id: LogActionId,
        action: LogAction<Ent>,
    ) -> Result<(), SummersetError> {
        self.tx_log
            .send((id, action))
            .map_err(|e| SummersetError(e.to_string()))
    }

    /// Waits for the next logging result by receiving from the ack channel.
    pub async fn get_result(
        &mut self,
    ) -> Result<(LogActionId, LogResult<Ent>), SummersetError> {
        match self.rx_ack.recv().await {
            Some((id, result)) => Ok((id, result)),
            None => logged_err!(self.me; "ack channel has been closed"),
        }
    }

    /// Try to get the next logging result using `try_recv()`.
    #[allow(dead_code)]
    pub fn try_get_result(
        &mut self,
    ) -> Result<(LogActionId, LogResult<Ent>), SummersetError> {
        match self.rx_ack.try_recv() {
            Ok((id, result)) => Ok((id, result)),
            Err(e) => Err(SummersetError(e.to_string())),
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
    ) -> Result<(Option<Ent>, usize), SummersetError> {
        if offset + 8 > file_size {
            if offset < file_size {
                // suppress warning if offset == file_size to avoid excessive
                // log lines during recovery
                pf_warn!(
                    me;
                    "read header end offset {} out of file bound {}",
                    offset + 8,
                    file_size
                );
            }
            return Ok((None, offset));
        }

        // read entry length header
        backer.seek(SeekFrom::Start(offset as u64)).await?;
        let entry_len: usize = backer.read_u64().await? as usize;
        let offset_e = offset + 8 + entry_len;
        if offset_e > file_size {
            pf_warn!(me; "read entry invalid length {}", entry_len);
            backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF
            return Ok((None, offset));
        }

        // read entry content
        let mut entry_buf: Vec<u8> = vec![0; entry_len];
        backer.read_exact(&mut entry_buf[..]).await?;
        let entry = decode_from_slice(&entry_buf)?;
        backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF
        Ok((Some(entry), offset_e))
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

    /// Discard the file before given index, keeping the tail part (and
    /// optionally a head part).
    async fn discard_log(
        me: ReplicaId,
        backer: &mut File,
        file_size: usize,
        offset: usize,
        keep: usize,
    ) -> Result<(bool, usize), SummersetError> {
        if offset > file_size {
            pf_warn!(
                me;
                "discard offset {} exceeds file end {}",
                offset,
                file_size
            );
            Ok((false, file_size))
        } else if keep >= offset {
            pf_warn!(
                me;
                "discard keeping {} while offset is {}",
                keep, offset
            );
            Ok((false, file_size))
        } else {
            let tail_size = file_size - offset;
            if tail_size > 0 {
                // due to the limited interfaces provided by `tokio::fs`, we
                // read out the tail part and write it back to offset keep to
                // achieve the effect of discarding
                let mut tail_buf: Vec<u8> = vec![0; tail_size];
                backer.seek(SeekFrom::Start(offset as u64)).await?;
                backer.read_exact(&mut tail_buf[..]).await?;

                backer.seek(SeekFrom::Start(keep as u64)).await?;
                backer.write_all(&tail_buf[..]).await?;
            }

            backer.set_len((keep + tail_size) as u64).await?;
            backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF

            backer.sync_all().await?;
            Ok((true, keep + tail_size))
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
                Self::read_entry(me, backer, *file_size, offset).await.map(
                    |(entry, end_offset)| LogResult::Read { entry, end_offset },
                )
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
            LogAction::Discard { offset, keep } => {
                Self::discard_log(me, backer, *file_size, offset, keep)
                    .await
                    .map(|(offset_ok, now_size)| {
                        *file_size = now_size;
                        LogResult::Discard {
                            offset_ok,
                            now_size,
                        }
                    })
            }
        }
    }

    /// Logger thread function.
    async fn logger_thread(
        me: ReplicaId,
        mut backer_file: File,
        mut rx_log: mpsc::UnboundedReceiver<(LogActionId, LogAction<Ent>)>,
        tx_ack: mpsc::UnboundedSender<(LogActionId, LogResult<Ent>)>,
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
            // pf_trace!(me; "log action {:?}", action);
            let res =
                Self::do_action(me, &mut backer_file, &mut file_size, action)
                    .await;
            if let Err(e) = res {
                pf_error!(me; "error during logging: {}", e);
                continue;
            }

            if let Err(e) = tx_ack.send((id, res.unwrap())) {
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

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, GetSize)]
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
        debug_assert!(offset_ok);
        let (offset_ok, now_size) = StorageHub::write_entry(
            0,
            &mut backer_file,
            now_size,
            &entry,
            now_size,
            false,
        )
        .await?;
        debug_assert!(offset_ok);
        let (offset_ok, now_size) = StorageHub::write_entry(
            0,
            &mut backer_file,
            now_size,
            &entry,
            0,
            true,
        )
        .await?;
        debug_assert!(offset_ok);
        let (offset_ok, _) = StorageHub::write_entry(
            0,
            &mut backer_file,
            now_size,
            &entry,
            now_size + 10,
            false,
        )
        .await?;
        debug_assert!(!offset_ok);
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
        debug_assert!(mid_size >= entry_bytes.len());
        let end_size = StorageHub::append_entry(
            0,
            &mut backer_file,
            mid_size,
            &entry,
            true,
        )
        .await?;
        debug_assert!(end_size - mid_size >= entry_bytes.len());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn read_entries() -> Result<(), SummersetError> {
        let mut backer_file =
            prepare_test_file("/tmp/test-backer-2.log").await?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let mid_size =
            StorageHub::append_entry(0, &mut backer_file, 0, &entry, false)
                .await?;
        let end_size = StorageHub::append_entry(
            0,
            &mut backer_file,
            mid_size,
            &entry,
            true,
        )
        .await?;
        assert_eq!(
            StorageHub::read_entry(0, &mut backer_file, end_size, mid_size)
                .await?,
            (Some(TestEntry("test-entry-dummy-string".into())), end_size)
        );
        assert_eq!(
            StorageHub::read_entry(0, &mut backer_file, end_size, 0).await?,
            (Some(TestEntry("test-entry-dummy-string".into())), mid_size)
        );
        assert_eq!(
            StorageHub::<TestEntry>::read_entry(
                0,
                &mut backer_file,
                end_size,
                mid_size + 10
            )
            .await?,
            (None, mid_size + 10)
        );
        assert_eq!(
            StorageHub::<TestEntry>::read_entry(
                0,
                &mut backer_file,
                mid_size,
                mid_size - 4
            )
            .await?,
            (None, mid_size - 4)
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
        let mid1_offset =
            StorageHub::append_entry(0, &mut backer_file, 0, &entry, false)
                .await?;
        let mid2_offset = StorageHub::append_entry(
            0,
            &mut backer_file,
            mid1_offset,
            &entry,
            false,
        )
        .await?;
        let end_offset = StorageHub::append_entry(
            0,
            &mut backer_file,
            mid2_offset,
            &entry,
            true,
        )
        .await?;
        let tail_size = end_offset - mid2_offset;
        assert_eq!(
            StorageHub::<TestEntry>::discard_log(
                0,
                &mut backer_file,
                end_offset,
                mid2_offset,
                mid1_offset,
            )
            .await?,
            (true, 2 * tail_size)
        );
        assert_eq!(
            StorageHub::<TestEntry>::discard_log(
                0,
                &mut backer_file,
                2 * tail_size,
                mid1_offset,
                end_offset,
            )
            .await?,
            (false, 2 * tail_size)
        );
        assert_eq!(
            StorageHub::<TestEntry>::discard_log(
                0,
                &mut backer_file,
                2 * tail_size,
                mid1_offset,
                0,
            )
            .await?,
            (true, tail_size)
        );
        assert_eq!(
            StorageHub::<TestEntry>::discard_log(
                0,
                &mut backer_file,
                tail_size,
                end_offset,
                0
            )
            .await?,
            (false, tail_size)
        );
        assert_eq!(
            StorageHub::<TestEntry>::discard_log(
                0,
                &mut backer_file,
                tail_size,
                tail_size,
                0
            )
            .await?,
            (true, 0)
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_log_ack() -> Result<(), SummersetError> {
        let path = Path::new("/tmp/test-backer-6.log");
        let mut hub = StorageHub::new_and_setup(0, path, None).await?;
        let entry = TestEntry("abcdefgh".into());
        let entry_bytes = encode_to_vec(&entry)?;
        hub.submit_action(0, LogAction::Append { entry, sync: true })?;
        hub.submit_action(1, LogAction::Read { offset: 0 })?;
        hub.submit_action(2, LogAction::Truncate { offset: 0 })?;
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
                    entry: Some(TestEntry("abcdefgh".into())),
                    end_offset: 8 + entry_bytes.len(),
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
