//! Summerset server durable storage logging module implementation.

use std::fmt;
use std::io::{Cursor, SeekFrom};
use std::path::{Path, PathBuf};

use get_size::GetSize;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time;
use wincode::{SchemaRead, SchemaWrite};
use xxhash_rust::xxh3::xxh3_64;

use crate::server::ReplicaId;
use crate::utils::faults::{self, DiskFail};
use crate::utils::{CODEC_CONFIG, Codec, SummersetError};

/// Header at the start of every backer file: a magic tag and the format
/// version, so that files in another format are refused instead of misread.
/// Offsets given to and returned by the logger do not count it.
const FILE_HEADER: &[u8; 8] = b"SMRLOG07";

/// Length of the head of each record: the entry length, its bitwise negation
/// (to tell a corrupted length), and the checksum of the entry.
const RECORD_HEAD: usize = 16;

/// Action command to the logger. File cursor will be positioned at EOF after
/// every action.
#[derive(Debug, Serialize, Deserialize, SchemaWrite, SchemaRead, GetSize)]
pub(crate) enum LogAction<Ent> {
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
    /// optionally a fixed head part).
    Discard { offset: usize, keep: usize },

    /// Do nothing, completing once all previously submitted actions have.
    Barrier,
}

/// Action result returned by the logger.
#[derive(Debug, Serialize, Deserialize, SchemaWrite, SchemaRead, PartialEq, GetSize)]
pub(crate) enum LogResult<Ent> {
    /// `Some(entry)` if successful, else `None`.
    Read {
        entry: Option<Ent>,
        end_offset: usize,
    },

    /// `ok` is true if offset is valid, else false. `now_size` is the size
    /// of file after this.
    Write { offset_ok: bool, now_size: usize },

    /// `offset` is where the entry starts, and `now_size` is the size of
    /// file after this.
    Append { offset: usize, now_size: usize },

    /// `ok` is true if truncate successful, else false. `now_size` is the
    /// size of file after this.
    Truncate { offset_ok: bool, now_size: usize },

    /// `ok` is true if discard successful, else false. `now_size` is the size
    /// of file after this.
    Discard { offset_ok: bool, now_size: usize },

    /// All previously submitted actions have completed.
    Barrier,
}

/// Reply of the logger to an action: the action's context (none if submitted
/// synchronously), and its result, or error if it failed.
type LogAck<Ent, Ctx> = (Option<Ctx>, Result<LogResult<Ent>, SummersetError>);

/// Durable storage logging module. Each action submitted asynchronously
/// carries a context of type `Ctx`, given back along with its result to tell
/// what the action was about; actions done synchronously carry none.
pub(crate) struct StorageHub<Ent, Ctx = ()> {
    /// My replica ID.
    _me: ReplicaId,

    /// Sender side of the log channel.
    tx_log: mpsc::UnboundedSender<(Option<Ctx>, LogAction<Ent>)>,

    /// Receiver side of the ack channel; every action gets a result, which is
    /// an error if the action failed.
    rx_ack: mpsc::UnboundedReceiver<LogAck<Ent, Ctx>>,

    /// Join handle of the logger task.
    _logger_handle: JoinHandle<()>,
}

// StorageHub public API implementation
impl<Ent, Ctx> StorageHub<Ent, Ctx>
where
    Ent:
        fmt::Debug + Clone + Serialize + DeserializeOwned + Codec + GetSize + Send + Sync + 'static,
    Ctx: Send + 'static,
{
    /// Creates a new durable storage logging hub. Spawns the logger task.
    /// Creates a log channel for submitting logging actions to the logger and
    /// an ack channel for getting results. Prepares the given backing file as
    /// durability backend.
    pub(crate) async fn new_and_setup(me: ReplicaId, path: &Path) -> Result<Self, SummersetError> {
        // prepare backing file
        if fs::try_exists(path).await? {
            pf_info!("backer file '{}' already exists", path.display());
        } else {
            File::create(path).await?;
            pf_info!("created backer file '{}'", path.display());
        }
        let mut backer_file = OpenOptions::new().read(true).write(true).open(path).await?;

        // after a power loss, the file loses what it had not synced
        let len = usize::try_from(backer_file.metadata().await?.len())?;
        if let Some(size) = faults::take_crash_size(path).filter(|&size| size < len) {
            pf_warn!(
                "power loss lost {} unsynced bytes of '{}'",
                len - size,
                path.display()
            );
            backer_file.set_len(size as u64).await?;
        }

        // a file without a complete header holds no records yet
        let len = usize::try_from(backer_file.metadata().await?.len())?;
        if len < FILE_HEADER.len() {
            backer_file.set_len(0).await?;
            backer_file.write_all(FILE_HEADER).await?;
            backer_file.sync_all().await?;
        } else {
            let mut header = [0; FILE_HEADER.len()];
            backer_file.read_exact(&mut header).await?;
            if &header != FILE_HEADER {
                return logged_err!("backer file '{}' has an unknown format", path.display());
            }
        }
        backer_file.seek(SeekFrom::End(0)).await?; // seek to EOF

        let (tx_log, rx_log) = mpsc::unbounded_channel();
        let (tx_ack, rx_ack) = mpsc::unbounded_channel();

        let mut logger = StorageHubLoggerTask::new(rx_log, tx_ack, backer_file, path).await?;
        let logger_handle = tokio::spawn(async move { logger.run().await });

        Ok(StorageHub {
            _me: me,
            tx_log,
            rx_ack,
            _logger_handle: logger_handle,
        })
    }

    /// Truncates the backing file to empty, as a synchronous action.
    pub(crate) async fn truncate_all(&mut self) -> Result<(), SummersetError> {
        let (_, result) = self
            .do_sync_action(LogAction::Truncate { offset: 0 })
            .await?;
        if matches!(
            result,
            LogResult::Truncate {
                offset_ok: true,
                now_size: 0
            }
        ) {
            Ok(())
        } else {
            logged_err!("failed to truncate log to 0")
        }
    }

    /// Submits an action with given context by sending it to the log channel.
    pub(crate) fn submit_action(
        &mut self,
        ctx: Ctx,
        action: LogAction<Ent>,
    ) -> Result<(), SummersetError> {
        self.tx_log
            .send((Some(ctx), action))
            .map_err(SummersetError::msg)
    }

    /// Waits for the next logging result by receiving from the ack channel.
    /// Failed actions, already logged by the logger, are skipped.
    pub(crate) async fn get_result(&mut self) -> Result<(Ctx, LogResult<Ent>), SummersetError> {
        loop {
            match self.rx_ack.recv().await {
                Some((Some(ctx), Ok(result))) => return Ok((ctx, result)),
                Some((Some(_), Err(_))) => {}
                Some((None, _)) => pf_error!("unexpected result of a synchronous action"),
                None => return logged_err!("ack channel has been closed"),
            }
        }
    }

    /// Submits an action and waits for its result blockingly.
    /// Returns a tuple where the first element is a vec containing any old
    /// results of previously submitted actions received in the middle and
    /// the second element is the result of this sync action.
    pub(crate) async fn do_sync_action(
        &mut self,
        action: LogAction<Ent>,
    ) -> Result<(Vec<(Ctx, LogResult<Ent>)>, LogResult<Ent>), SummersetError> {
        self.tx_log
            .send((None, action))
            .map_err(SummersetError::msg)?;
        let mut old_results = vec![];
        loop {
            // results come in submission order, and no other synchronous
            // action can be pending, so the first one without context is mine
            match self.rx_ack.recv().await {
                Some((Some(ctx), Ok(result))) => old_results.push((ctx, result)),
                Some((Some(_), Err(e))) => pf_error!("error of a log action: {}", e),
                Some((None, result)) => return Ok((old_results, result?)),
                None => return logged_err!("ack channel has been closed"),
            }
        }
    }

    /// Waits for all previously submitted actions to complete, returning their
    /// results not received yet.
    pub(crate) async fn barrier(&mut self) -> Result<Vec<(Ctx, LogResult<Ent>)>, SummersetError> {
        let (old_results, result) = self.do_sync_action(LogAction::Barrier).await?;
        debug_assert!(matches!(result, LogResult::Barrier));
        Ok(old_results)
    }
}

/// `StorageHub` durable logger task.
struct StorageHubLoggerTask<Ent, Ctx> {
    rx_log: mpsc::UnboundedReceiver<(Option<Ctx>, LogAction<Ent>)>,
    tx_ack: mpsc::UnboundedSender<LogAck<Ent, Ctx>>,

    backer_file: File,
    /// Backer file size (including the header) is maintained by the logger.
    file_size: usize,

    /// Backer file path, and the size up to which it has been synced.
    path: PathBuf,
    synced_size: usize,
}

impl<Ent, Ctx> StorageHubLoggerTask<Ent, Ctx>
where
    Ent: fmt::Debug + Clone + Serialize + DeserializeOwned + Codec + Send + Sync + 'static,
{
    /// Creates the durable logger task.
    async fn new(
        rx_log: mpsc::UnboundedReceiver<(Option<Ctx>, LogAction<Ent>)>,
        tx_ack: mpsc::UnboundedSender<LogAck<Ent, Ctx>>,
        backer_file: File,
        path: &Path,
    ) -> Result<Self, SummersetError> {
        // load initial file size
        let metadata = backer_file.metadata().await;
        if let Err(e) = metadata {
            return logged_err!("error reading backer file metadata: {}, exiting", e);
        }
        #[allow(clippy::cast_possible_truncation)]
        let file_size: usize = metadata.unwrap().len() as usize;

        // whatever the file already holds is considered synced
        faults::record_file_synced(path, file_size);
        Ok(StorageHubLoggerTask {
            rx_log,
            tx_ack,
            backer_file,
            file_size,
            path: path.into(),
            synced_size: file_size,
        })
    }

    /// Encodes an entry into a record: its head, then itself, serialized in
    /// place after the head.
    fn encode_record(entry: &Ent) -> Result<Vec<u8>, SummersetError> {
        let size = usize::try_from(wincode::config::serialized_size(entry, CODEC_CONFIG)?)?;
        let mut record = Vec::with_capacity(RECORD_HEAD + size);
        record.resize(RECORD_HEAD, 0);
        let mut cursor = Cursor::new(&mut record);
        cursor.set_position(RECORD_HEAD as u64);
        wincode::config::serialize_into(&mut cursor, entry, CODEC_CONFIG)?;

        let Ok(len) = u32::try_from(record.len() - RECORD_HEAD) else {
            return logged_err!("entry of {} bytes too large", record.len() - RECORD_HEAD);
        };
        let checksum = xxh3_64(&record[RECORD_HEAD..]);
        record[..4].copy_from_slice(&len.to_be_bytes());
        record[4..8].copy_from_slice(&(!len).to_be_bytes());
        record[8..RECORD_HEAD].copy_from_slice(&checksum.to_be_bytes());
        Ok(record)
    }

    /// Parses a record head into the entry length and checksum, or `None` if
    /// the length is corrupted.
    fn parse_head(head: [u8; RECORD_HEAD]) -> Option<(usize, u64)> {
        let [l0, l1, l2, l3, n0, n1, n2, n3, checksum @ ..] = head;
        let len = u32::from_be_bytes([l0, l1, l2, l3]);
        (u32::from_be_bytes([n0, n1, n2, n3]) == !len)
            .then(|| (len as usize, u64::from_be_bytes(checksum)))
    }

    /// Read out entry at given offset. Returns `None` at the end of the log,
    /// which includes a last record cut short or corrupted (as by a crash amid
    /// writing it), but an error for a corrupted record followed by others or
    /// a corrupted record head (whose length is untrustworthy).
    /// This is a non-method function to make tests easier to write.
    async fn read_entry(
        backer: &mut File,
        file_size: usize,
        offset: usize,
    ) -> Result<(Option<Ent>, usize), SummersetError> {
        let read = async {
            if offset + RECORD_HEAD > file_size {
                if offset < file_size {
                    // suppress warning if offset == file_size to avoid excessive
                    // log lines during recovery
                    pf_warn!("incomplete record head at offset {}", offset);
                }
                return Ok((None, offset));
            }

            // read the record head at once
            let mut head = [0; RECORD_HEAD];
            backer.seek(SeekFrom::Start(offset as u64)).await?;
            backer.read_exact(&mut head).await?;
            let Some((entry_len, checksum)) = Self::parse_head(head) else {
                return logged_err!("corrupted record head at offset {}", offset);
            };
            let offset_e = offset + RECORD_HEAD + entry_len;
            if offset_e > file_size {
                pf_warn!(
                    "incomplete record of length {} at offset {}",
                    entry_len,
                    offset
                );
                return Ok((None, offset));
            }

            // read entry content (into a buffer not zeroed first) and verify it
            let mut entry_buf = Vec::with_capacity(entry_len);
            (&mut *backer)
                .take(entry_len as u64)
                .read_to_end(&mut entry_buf)
                .await?;
            if entry_buf.len() < entry_len || xxh3_64(&entry_buf) != checksum {
                if offset_e < file_size {
                    return logged_err!("corrupted record at offset {}", offset);
                }
                pf_warn!("corrupted last record at offset {}", offset);
                return Ok((None, offset));
            }
            let entry: Ent = wincode::config::deserialize_exact(&entry_buf, CODEC_CONFIG)?;
            Ok((Some(entry), offset_e))
        };
        let result = read.await;
        backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF, even upon errors
        result
    }

    /// Write given entry to given offset: either at the end, or in place of
    /// a record of the same length (as others may follow it).
    /// This is a non-method function to make tests easier to write.
    async fn write_entry(
        backer: &mut File,
        file_size: usize,
        entry: &Ent,
        offset: usize,
        sync: bool,
    ) -> Result<(bool, usize), SummersetError> {
        if offset > file_size {
            // disallow holes in log file
            pf_warn!("write offset {} out of file bound {}", offset, file_size);
            return Ok((false, file_size));
        }

        let record = Self::encode_record(entry)?;
        if offset < file_size {
            let replaces = offset + RECORD_HEAD <= file_size && {
                let mut head = [0; RECORD_HEAD];
                backer.seek(SeekFrom::Start(offset as u64)).await?;
                backer.read_exact(&mut head).await?;
                backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF
                Self::parse_head(head)
                    .is_some_and(|(old_len, _)| old_len == record.len() - RECORD_HEAD)
            };
            if !replaces {
                pf_warn!(
                    "write at offset {} does not replace a same-length record",
                    offset
                );
                return Ok((false, file_size));
            }
        }

        backer.seek(SeekFrom::Start(offset as u64)).await?;
        backer.write_all(&record).await?;
        backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF

        if sync {
            backer.sync_data().await?;
        }

        Ok((true, file_size.max(offset + record.len())))
    }

    /// Append given entry to EOF.
    /// This is a non-method function to make tests easier to write.
    async fn append_entry(
        backer: &mut File,
        file_size: usize,
        entry: &Ent,
        sync: bool,
    ) -> Result<usize, SummersetError> {
        let record = Self::encode_record(entry)?;
        backer.write_all(&record).await?;

        if sync {
            backer.sync_data().await?;
        }

        Ok(file_size + record.len())
    }

    /// Truncate the file at given index, keeping the head part.
    /// This is a non-method function to make tests easier to write.
    async fn truncate_log(
        backer: &mut File,
        file_size: usize,
        offset: usize,
    ) -> Result<(bool, usize), SummersetError> {
        if offset > file_size {
            pf_warn!("truncate offset {} exceeds file end {}", offset, file_size);
            Ok((false, file_size))
        } else {
            backer.set_len(offset as u64).await?;
            backer.seek(SeekFrom::End(0)).await?; // recover cursor to EOF

            backer.sync_all().await?;
            Ok((true, offset))
        }
    }

    /// Discard the file before given index, keeping the tail part (and
    /// optionally a fixed head part).
    /// This is a non-method function to make tests easier to write.
    async fn discard_log(
        backer: &mut File,
        file_size: usize,
        offset: usize,
        keep: usize,
    ) -> Result<(bool, usize), SummersetError> {
        if offset > file_size {
            pf_warn!("discard offset {} exceeds file end {}", offset, file_size);
            Ok((false, file_size))
        } else if keep >= offset {
            pf_warn!("discard keeping {} while offset is {}", keep, offset);
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

    /// Synthesized handler of durable logging actions on logger. Offsets in
    /// actions and results do not count the file header.
    async fn handle_action(
        &mut self,
        action: LogAction<Ent>,
    ) -> Result<LogResult<Ent>, SummersetError> {
        let head = FILE_HEADER.len();
        match action {
            LogAction::Read { offset } => {
                Self::read_entry(&mut self.backer_file, self.file_size, head + offset)
                    .await
                    .map(|(entry, end_offset)| LogResult::Read {
                        entry,
                        end_offset: end_offset - head,
                    })
            }
            LogAction::Write {
                entry,
                offset,
                sync,
            } => Self::write_entry(
                &mut self.backer_file,
                self.file_size,
                &entry,
                head + offset,
                sync,
            )
            .await
            .map(|(offset_ok, now_size)| {
                self.file_size = now_size;
                LogResult::Write {
                    offset_ok,
                    now_size: now_size - head,
                }
            }),
            LogAction::Append { entry, sync } => {
                Self::append_entry(&mut self.backer_file, self.file_size, &entry, sync)
                    .await
                    .map(|now_size| {
                        let offset = self.file_size - head;
                        self.file_size = now_size;
                        LogResult::Append {
                            offset,
                            now_size: now_size - head,
                        }
                    })
            }
            LogAction::Truncate { offset } => {
                Self::truncate_log(&mut self.backer_file, self.file_size, head + offset)
                    .await
                    .map(|(offset_ok, now_size)| {
                        self.file_size = now_size;
                        LogResult::Truncate {
                            offset_ok,
                            now_size: now_size - head,
                        }
                    })
            }
            LogAction::Discard { offset, keep } => Self::discard_log(
                &mut self.backer_file,
                self.file_size,
                head + offset,
                head + keep,
            )
            .await
            .map(|(offset_ok, now_size)| {
                self.file_size = now_size;
                LogResult::Discard {
                    offset_ok,
                    now_size: now_size - head,
                }
            }),
            LogAction::Barrier => Ok(LogResult::Barrier),
        }
    }

    /// Cuts off whatever a failed write left past the last known end of file,
    /// and puts the cursor back there.
    async fn restore_end(&mut self) -> Result<(), SummersetError> {
        self.backer_file.set_len(self.file_size as u64).await?;
        self.backer_file.seek(SeekFrom::End(0)).await?;
        Ok(())
    }

    /// Starts the durable logger task loop.
    async fn run(&mut self) {
        pf_debug!("logger task spawned");

        while let Some((ctx, action)) = self.rx_log.recv().await {
            // pf_trace!("log action {:?}", action);
            let syncs = match action {
                LogAction::Read { .. } | LogAction::Barrier => false,
                LogAction::Write { sync, .. } | LogAction::Append { sync, .. } => sync,
                LogAction::Truncate { .. } | LogAction::Discard { .. } => true,
            };
            let write = !matches!(action, LogAction::Read { .. } | LogAction::Barrier);
            let append = matches!(action, LogAction::Append { .. });
            let (delay, fail) = faults::disk_delay_fail(write).unwrap_or_default();
            if !delay.is_zero() {
                time::sleep(delay).await;
            }

            let start = self.file_size;
            let mut res = if fail == Some(DiskFail::Error) {
                Err(SummersetError::msg("injected disk failure"))
            } else {
                self.handle_action(action).await
            };
            if res.is_ok() && append {
                match fail {
                    Some(DiskFail::Torn) => {
                        // as if only part of the record got written
                        self.file_size = start;
                        res = Err(SummersetError::msg("injected torn write"));
                    }
                    Some(DiskFail::Flip(bit)) => {
                        let entry = (start + RECORD_HEAD)..self.file_size;
                        let flipped =
                            faults::flip_file_bit(&mut self.backer_file, entry, bit).await;
                        if let Err(e) = flipped {
                            res = Err(e);
                        }
                    }
                    _ => {}
                }
            }
            if let Err(e) = &res {
                pf_error!("error during logging: {}", e);
                // drop whatever the failed action left, so that later actions
                // see a consistent file
                if let Err(e) = self.restore_end().await {
                    pf_error!("error restoring backer file end: {}", e);
                }
            }

            // track how much of the file is synced, for crash injection
            let synced_size = if syncs && res.is_ok() {
                self.file_size
            } else {
                self.synced_size.min(self.file_size)
            };
            if synced_size != self.synced_size {
                self.synced_size = synced_size;
                faults::record_file_synced(&self.path, synced_size);
            }

            if let Err(e) = self.tx_ack.send((ctx, res)) {
                pf_error!("error sending to tx_ack: {}", e);
            }
        }

        // channel gets closed and no messages remain
        pf_debug!("logger task exited");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(
        Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaWrite, SchemaRead, GetSize,
    )]
    struct TestEntry(String);

    type Logger = StorageHubLoggerTask<TestEntry, ()>;

    /// Creates an anonymous file that is deleted once closed.
    fn test_temp_file() -> Result<File, SummersetError> {
        Ok(File::from_std(tempfile::tempfile()?))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn write_entries() -> Result<(), SummersetError> {
        let mut backer_file = test_temp_file()?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let (offset_ok, now_size) =
            Logger::write_entry(&mut backer_file, 0, &entry, 0, false).await?;
        debug_assert!(offset_ok);
        let (offset_ok, now_size) =
            Logger::write_entry(&mut backer_file, now_size, &entry, now_size, false).await?;
        debug_assert!(offset_ok);
        let (offset_ok, now_size) =
            Logger::write_entry(&mut backer_file, now_size, &entry, 0, true).await?;
        debug_assert!(offset_ok);
        let (offset_ok, _) =
            Logger::write_entry(&mut backer_file, now_size, &entry, now_size + 10, false).await?;
        debug_assert!(!offset_ok);
        // an in-place write of another length would clobber what follows
        let longer = TestEntry("test-entry-dummy-string-longer".into());
        let (offset_ok, _) =
            Logger::write_entry(&mut backer_file, now_size, &longer, 0, false).await?;
        debug_assert!(!offset_ok);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn append_entries() -> Result<(), SummersetError> {
        let mut backer_file = test_temp_file()?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let entry_bytes = wincode::config::serialize(&entry, CODEC_CONFIG)?;
        let mid_size = Logger::append_entry(&mut backer_file, 0, &entry, false).await?;
        debug_assert!(mid_size >= entry_bytes.len());
        let end_size = Logger::append_entry(&mut backer_file, mid_size, &entry, true).await?;
        debug_assert!(end_size - mid_size >= entry_bytes.len());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn read_entries() -> Result<(), SummersetError> {
        let mut backer_file = test_temp_file()?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let mid_size = Logger::append_entry(&mut backer_file, 0, &entry, false).await?;
        let end_size = Logger::append_entry(&mut backer_file, mid_size, &entry, true).await?;
        assert_eq!(
            Logger::read_entry(&mut backer_file, end_size, mid_size).await?,
            (Some(TestEntry("test-entry-dummy-string".into())), end_size)
        );
        assert_eq!(
            Logger::read_entry(&mut backer_file, end_size, 0).await?,
            (Some(TestEntry("test-entry-dummy-string".into())), mid_size)
        );
        // amid a record, the head read is garbage
        assert!(
            Logger::read_entry(&mut backer_file, end_size, mid_size + 10)
                .await
                .is_err()
        );
        assert_eq!(
            Logger::read_entry(&mut backer_file, mid_size, mid_size - 4).await?,
            (None, mid_size - 4)
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn read_cut_points() -> Result<(), SummersetError> {
        // a log cut at any byte (e.g., by a crash) reads back as exactly its
        // complete records
        let entry = TestEntry("test-entry-dummy-string".into());
        let mut backer_file = test_temp_file()?;
        let mut ends = vec![0];
        for _ in 0..3 {
            let end = *ends.last().unwrap();
            ends.push(Logger::append_entry(&mut backer_file, end, &entry, false).await?);
        }
        let mut image = vec![];
        backer_file.seek(SeekFrom::Start(0)).await?;
        backer_file.read_to_end(&mut image).await?;
        for cut in 0..=image.len() {
            let mut cut_file = test_temp_file()?;
            cut_file.write_all(&image[..cut]).await?;
            let (mut offset, mut num_read) = (0, 0);
            while let (Some(read), end) = Logger::read_entry(&mut cut_file, cut, offset).await? {
                assert_eq!(read, entry);
                (offset, num_read) = (end, num_read + 1);
            }
            assert_eq!(
                num_read,
                ends[1..].iter().filter(|&&end| end <= cut).count()
            );
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn read_detects_corruption() -> Result<(), SummersetError> {
        // a corrupted record is an error amid the log, but the end of the log
        // (as when torn by a crash) if it is the last one
        let entry = TestEntry("test-entry-dummy-string".into());
        let mut backer_file = test_temp_file()?;
        let mid_size = Logger::append_entry(&mut backer_file, 0, &entry, false).await?;
        let end_size = Logger::append_entry(&mut backer_file, mid_size, &entry, false).await?;
        for (offset, last) in [(0, false), (mid_size, true)] {
            let pos = (offset + RECORD_HEAD + 3) as u64;
            backer_file.seek(SeekFrom::Start(pos)).await?;
            backer_file.write_all(b"?").await?;
            let read = Logger::read_entry(&mut backer_file, end_size, offset).await;
            if last {
                assert_eq!(read?, (None, offset));
            } else {
                assert!(read.is_err());
            }
        }

        // a corrupted length is an error too, even if it points past the end
        let mut backer_file = test_temp_file()?;
        let mid_size = Logger::append_entry(&mut backer_file, 0, &entry, false).await?;
        let end_size = Logger::append_entry(&mut backer_file, mid_size, &entry, false).await?;
        backer_file.seek(SeekFrom::Start(0)).await?;
        backer_file.write_all(&[0xff]).await?;
        assert!(
            Logger::read_entry(&mut backer_file, end_size, 0)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn truncate_log() -> Result<(), SummersetError> {
        let mut backer_file = test_temp_file()?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let mid_offset = Logger::append_entry(&mut backer_file, 0, &entry, false).await?;
        let end_offset = Logger::append_entry(&mut backer_file, mid_offset, &entry, true).await?;
        assert_eq!(
            Logger::truncate_log(&mut backer_file, end_offset, mid_offset).await?,
            (true, mid_offset)
        );
        assert_eq!(
            Logger::truncate_log(&mut backer_file, mid_offset, end_offset).await?,
            (false, mid_offset)
        );
        assert_eq!(
            Logger::truncate_log(&mut backer_file, mid_offset, 0).await?,
            (true, 0)
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn discard_log() -> Result<(), SummersetError> {
        let mut backer_file = test_temp_file()?;
        let entry = TestEntry("test-entry-dummy-string".into());
        let mid1_offset = Logger::append_entry(&mut backer_file, 0, &entry, false).await?;
        let mid2_offset =
            Logger::append_entry(&mut backer_file, mid1_offset, &entry, false).await?;
        let end_offset = Logger::append_entry(&mut backer_file, mid2_offset, &entry, true).await?;
        let tail_size = end_offset - mid2_offset;
        assert_eq!(
            Logger::discard_log(&mut backer_file, end_offset, mid2_offset, mid1_offset,).await?,
            (true, 2 * tail_size)
        );
        assert_eq!(
            Logger::discard_log(&mut backer_file, 2 * tail_size, mid1_offset, end_offset,).await?,
            (false, 2 * tail_size)
        );
        assert_eq!(
            Logger::discard_log(&mut backer_file, 2 * tail_size, mid1_offset, 0,).await?,
            (true, tail_size)
        );
        assert_eq!(
            Logger::discard_log(&mut backer_file, tail_size, end_offset, 0).await?,
            (false, tail_size)
        );
        assert_eq!(
            Logger::discard_log(&mut backer_file, tail_size, tail_size, 0).await?,
            (true, 0)
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn api_log_ack() -> Result<(), SummersetError> {
        let dir = tempfile::tempdir()?;
        let mut hub = StorageHub::new_and_setup(0, &dir.path().join("backer.log")).await?;
        let entry = TestEntry("abcdefgh".into());
        let entry_bytes = wincode::config::serialize(&entry, CODEC_CONFIG)?;
        hub.submit_action(0, LogAction::Append { entry, sync: true })?;
        hub.submit_action(9, LogAction::Read { offset: 1 })?; // fails, skipped
        hub.submit_action(1, LogAction::Read { offset: 0 })?;
        hub.submit_action(2, LogAction::Truncate { offset: 0 })?;
        assert_eq!(
            hub.get_result().await?,
            (
                0,
                LogResult::Append {
                    offset: 0,
                    now_size: RECORD_HEAD + entry_bytes.len()
                }
            )
        );
        assert_eq!(
            hub.get_result().await?,
            (
                1,
                LogResult::Read {
                    entry: Some(TestEntry("abcdefgh".into())),
                    end_offset: RECORD_HEAD + entry_bytes.len(),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn api_do_sync() -> Result<(), SummersetError> {
        let dir = tempfile::tempdir()?;
        let mut hub = StorageHub::new_and_setup(0, &dir.path().join("backer.log")).await?;
        let entry = TestEntry("abcdefgh".into());
        let entry_bytes = wincode::config::serialize(&entry, CODEC_CONFIG)?;
        hub.submit_action(0, LogAction::Append { entry, sync: true })?;
        hub.submit_action(1, LogAction::Read { offset: 0 })?;
        assert_eq!(
            hub.do_sync_action(LogAction::Truncate { offset: 0 })
                .await?,
            (
                vec![
                    (
                        0,
                        LogResult::Append {
                            offset: 0,
                            now_size: RECORD_HEAD + entry_bytes.len()
                        }
                    ),
                    (
                        1,
                        LogResult::Read {
                            entry: Some(TestEntry("abcdefgh".into())),
                            end_offset: RECORD_HEAD + entry_bytes.len(),
                        }
                    )
                ],
                LogResult::Truncate {
                    offset_ok: true,
                    now_size: 0
                }
            )
        );
        // a barrier waits on, and returns, all earlier results, which tell
        // where each appended entry starts
        let entry = TestEntry("ijkl".into());
        let entry_len = wincode::config::serialize(&entry, CODEC_CONFIG)?.len();
        hub.submit_action(
            3,
            LogAction::Append {
                entry: entry.clone(),
                sync: false,
            },
        )?;
        hub.submit_action(4, LogAction::Append { entry, sync: false })?;
        let results = hub.barrier().await?;
        assert_eq!(
            results,
            vec![
                (
                    3,
                    LogResult::Append {
                        offset: 0,
                        now_size: RECORD_HEAD + entry_len
                    }
                ),
                (
                    4,
                    LogResult::Append {
                        offset: RECORD_HEAD + entry_len,
                        now_size: 2 * (RECORD_HEAD + entry_len)
                    }
                )
            ]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn api_file_header_and_errors() -> Result<(), SummersetError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("backer.log");
        let entry = TestEntry("abcdefgh".into());
        let mut hub = StorageHub::<TestEntry>::new_and_setup(0, &path).await?;
        for _ in 0..2 {
            hub.do_sync_action(LogAction::Append {
                entry: entry.clone(),
                sync: true,
            })
            .await?;
        }
        drop(hub);
        // entries read back after reopening, at offsets not counting the header
        let mut hub = StorageHub::<TestEntry>::new_and_setup(0, &path).await?;
        let (_, read) = hub.do_sync_action(LogAction::Read { offset: 0 }).await?;
        let LogResult::Read {
            entry: Some(read),
            end_offset,
        } = read
        else {
            panic!("unexpected read result {read:?}");
        };
        assert_eq!(read, entry);
        drop(hub);
        // a failed action returns its error, and the logger goes on
        let mut file = OpenOptions::new().write(true).open(&path).await?;
        file.seek(SeekFrom::Start((FILE_HEADER.len() + RECORD_HEAD) as u64))
            .await?;
        file.write_all(b"?").await?;
        let mut hub = StorageHub::<TestEntry>::new_and_setup(0, &path).await?;
        assert!(
            hub.do_sync_action(LogAction::Read { offset: 0 })
                .await
                .is_err()
        );
        let (_, read) = hub
            .do_sync_action(LogAction::Read { offset: end_offset })
            .await?;
        assert!(matches!(read, LogResult::Read { entry: Some(_), .. }));
        drop(hub);
        // a file in an unknown format is refused
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(b"NOTALOG!").await?;
        assert!(
            StorageHub::<TestEntry>::new_and_setup(0, &path)
                .await
                .is_err()
        );
        Ok(())
    }
}
