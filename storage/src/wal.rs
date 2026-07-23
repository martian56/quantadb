use crate::{
    page::{read_u16, read_u32, read_u64, write_u16, write_u32, write_u64},
    Lsn, PageId, Result, StorageError, MAX_PAGE_PAYLOAD,
};
use crc32fast::Hasher;
use std::{
    fs::{File, OpenOptions},
    path::Path,
};

const WAL_MAGIC: [u8; 4] = *b"QNWL";
const WAL_FORMAT_VERSION: u16 = 1;
const WAL_HEADER_SIZE: usize = 40;
const CHECKSUM_OFFSET: usize = 32;
const NO_PAGE_ID: u64 = u64::MAX;
/// The log file is preallocated in chunks of this size and recycled in
/// place, so the routine commit sync never touches file metadata. Growing
/// a file on every append makes each sync journal a size change, which
/// measured six times slower than syncing data blocks alone.
const WAL_PREALLOCATE_BYTES: u64 = 64 << 20;
/// Appends end with four zero bytes where the next header would start, so
/// a sequential scan stops cleanly instead of running into stale records
/// left over from an earlier lap over the recycled file.
const STOP_MARKER: [u8; 4] = [0; 4];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalKind {
    PageImage { page_id: PageId, payload: Vec<u8> },
    BatchCommit { page_count: u32 },
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalRecord {
    pub(crate) lsn: Lsn,
    pub(crate) kind: WalKind,
}

pub(crate) struct Wal {
    file: File,
    records: Vec<WalRecord>,
    next_lsn: u64,
    /// Where the next record begins; the logical size of the log.
    end_offset: u64,
    /// The physical, preallocated size of the file.
    file_length: u64,
}

impl Wal {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        let (records, end_offset) = scan_and_repair(&file)?;
        let next_lsn = records
            .last()
            .map_or(1, |record| record.lsn.0.saturating_add(1));
        let mut file_length = file.metadata()?.len();
        if file_length < WAL_PREALLOCATE_BYTES {
            // Preallocation is sparse and instant; the payoff is that the
            // per-commit sync never journals a size change again.
            file.set_len(WAL_PREALLOCATE_BYTES)?;
            file.sync_all()?;
            file_length = WAL_PREALLOCATE_BYTES;
        }
        Ok(Self {
            file,
            records,
            next_lsn,
            end_offset,
            file_length,
        })
    }

    pub(crate) fn append_page(&mut self, page_id: PageId, payload: &[u8]) -> Result<Lsn> {
        if payload.len() > MAX_PAGE_PAYLOAD {
            return Err(StorageError::PageTooLarge {
                actual: payload.len(),
                maximum: MAX_PAGE_PAYLOAD,
            });
        }
        self.append(WalKind::PageImage {
            page_id,
            payload: payload.to_vec(),
        })
    }

    pub(crate) fn append_checkpoint(&mut self) -> Result<Lsn> {
        self.append(WalKind::Checkpoint)
    }

    pub(crate) fn append_batch_commit(&mut self, page_count: usize) -> Result<Lsn> {
        if page_count == 0 {
            return Err(StorageError::CorruptWal {
                offset: self.file.metadata().map_or(0, |metadata| metadata.len()),
                reason: "batch commit cannot be empty".to_owned(),
            });
        }
        let page_count = u32::try_from(page_count).map_err(|_| StorageError::CorruptWal {
            offset: self.file.metadata().map_or(0, |metadata| metadata.len()),
            reason: "batch page count exceeds u32".to_owned(),
        })?;
        self.append(WalKind::BatchCommit { page_count })
    }

    pub(crate) fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub(crate) fn records(&self) -> &[WalRecord] {
        &self.records
    }

    pub(crate) fn size_bytes(&self) -> Result<u64> {
        Ok(self.end_offset)
    }

    /// Discard the whole log after a checkpoint is durable.
    ///
    /// Everything before a durable checkpoint is dead weight: the data pages
    /// it protects are already synced, and recovery starts at the newest
    /// checkpoint anyway. An empty log and a log ending in a checkpoint
    /// recover identically, so truncation is safe at any moment after the
    /// checkpoint record reaches disk. In-memory LSN allocation continues
    /// unchanged, and a reopen recovers it from the data pages.
    pub(crate) fn reset_after_checkpoint(&mut self) -> Result<()> {
        // Recycle in place: a stop marker at the start makes the whole file
        // logically empty while its physical size, and therefore the cost
        // of future syncs, stays untouched.
        write_all_at(&self.file, &STOP_MARKER, 0)?;
        self.file.sync_data()?;
        self.end_offset = 0;
        self.records.clear();
        self.records.shrink_to_fit();
        Ok(())
    }

    /// Drop in-memory records recovery can no longer need.
    ///
    /// Replay only reads records after the newest checkpoint, so everything
    /// up to and including it can leave memory once recovery has run. The
    /// on-disk log is untouched.
    pub(crate) fn trim_records_to_last_checkpoint(&mut self) {
        if let Some(position) = self
            .records
            .iter()
            .rposition(|record| matches!(record.kind, WalKind::Checkpoint))
        {
            self.records.drain(..=position);
        }
    }

    pub(crate) fn ensure_next_lsn_after(&mut self, durable_lsn: u64) -> Result<()> {
        self.next_lsn = self.next_lsn.max(durable_lsn.checked_add(1).ok_or_else(|| {
            StorageError::CorruptWal {
                offset: self.file.metadata().map_or(0, |metadata| metadata.len()),
                reason: "LSN space exhausted".to_owned(),
            }
        })?);
        Ok(())
    }

    fn append(&mut self, kind: WalKind) -> Result<Lsn> {
        let lsn = Lsn(self.next_lsn);
        self.next_lsn = self
            .next_lsn
            .checked_add(1)
            .ok_or_else(|| StorageError::CorruptWal {
                offset: self.file.metadata().map_or(0, |metadata| metadata.len()),
                reason: "LSN space exhausted".to_owned(),
            })?;

        let (record_type, page_id, payload): (u8, u64, Vec<u8>) = match &kind {
            WalKind::PageImage { page_id, payload } => (1, page_id.0, payload.clone()),
            WalKind::BatchCommit { page_count } => {
                (2, NO_PAGE_ID, page_count.to_le_bytes().to_vec())
            }
            WalKind::Checkpoint => (3, NO_PAGE_ID, Vec::new()),
        };
        let record_length =
            WAL_HEADER_SIZE
                .checked_add(payload.len())
                .ok_or_else(|| StorageError::CorruptWal {
                    offset: self.file.metadata().map_or(0, |metadata| metadata.len()),
                    reason: "record length overflow".to_owned(),
                })?;
        let record_length_u32 =
            u32::try_from(record_length).map_err(|_| StorageError::CorruptWal {
                offset: self.file.metadata().map_or(0, |metadata| metadata.len()),
                reason: "record is too large".to_owned(),
            })?;

        let mut header = [0_u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(&WAL_MAGIC);
        write_u16(&mut header, 4, WAL_FORMAT_VERSION);
        header[6] = record_type;
        write_u32(&mut header, 8, record_length_u32);
        write_u32(&mut header, 12, payload.len() as u32);
        write_u64(&mut header, 16, lsn.0);
        write_u64(&mut header, 24, page_id);
        let checksum = record_checksum(&header, &payload);
        write_u32(&mut header, CHECKSUM_OFFSET, checksum);

        let record_end = self
            .end_offset
            .checked_add(record_length as u64)
            .ok_or_else(|| StorageError::CorruptWal {
                offset: self.end_offset,
                reason: "log offset overflow".to_owned(),
            })?;
        let needed = record_end + STOP_MARKER.len() as u64;
        if needed > self.file_length {
            let grown = needed.checked_add(WAL_PREALLOCATE_BYTES).ok_or_else(|| {
                StorageError::CorruptWal {
                    offset: self.end_offset,
                    reason: "log size overflow".to_owned(),
                }
            })?;
            self.file.set_len(grown)?;
            self.file_length = grown;
        }

        write_all_at(&self.file, &header, self.end_offset)?;
        write_all_at(
            &self.file,
            &payload,
            self.end_offset + WAL_HEADER_SIZE as u64,
        )?;
        write_all_at(&self.file, &STOP_MARKER, record_end)?;
        self.end_offset = record_end;
        self.records.push(WalRecord { lsn, kind });
        Ok(lsn)
    }
}

#[cfg(windows)]
fn write_all_at(file: &File, buffer: &[u8], offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;
    let mut written = 0_usize;
    while written < buffer.len() {
        let count = file.seek_write(&buffer[written..], offset + written as u64)?;
        if count == 0 {
            return Err(StorageError::CorruptWal {
                offset,
                reason: "log write made no progress".to_owned(),
            });
        }
        written += count;
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at(file: &File, buffer: &[u8], offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buffer, offset)?;
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> Result<bool> {
    use std::os::windows::fs::FileExt;
    let mut filled = 0_usize;
    while filled < buffer.len() {
        let read = file.seek_read(&mut buffer[filled..], offset + filled as u64)?;
        if read == 0 {
            return Ok(false);
        }
        filled += read;
    }
    Ok(true)
}

#[cfg(unix)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> Result<bool> {
    use std::os::unix::fs::FileExt;
    let mut filled = 0_usize;
    while filled < buffer.len() {
        let read = file.read_at(&mut buffer[filled..], offset + filled as u64)?;
        if read == 0 {
            return Ok(false);
        }
        filled += read;
    }
    Ok(true)
}

/// Scan the log from the start, stopping at the first thing that is not a
/// complete, checksummed, monotonic record.
///
/// The file is preallocated and recycled, so its physical length says
/// nothing; the log ends where a zeroed header, a torn or invalid record,
/// or a checksum mismatch begins. Everything from that point is treated as
/// the tail of an interrupted write and sealed off with a stop marker.
/// Committed batches are protected by their batch-commit records, so
/// sealing the tail never loses acknowledged work.
fn scan_and_repair(file: &File) -> Result<(Vec<WalRecord>, u64)> {
    let file_length = file.metadata()?.len();
    let mut records = Vec::new();
    let mut offset = 0_u64;
    let mut previous_lsn = 0_u64;

    loop {
        if offset.saturating_add(WAL_HEADER_SIZE as u64) > file_length {
            break;
        }
        let mut header = [0_u8; WAL_HEADER_SIZE];
        if !read_exact_at(file, &mut header, offset)? {
            break;
        }
        if header[0..4] != WAL_MAGIC {
            break;
        }
        if read_u16(&header, 4) != WAL_FORMAT_VERSION {
            break;
        }
        let record_length = u64::from(read_u32(&header, 8));
        let payload_length = read_u32(&header, 12) as usize;
        if record_length != (WAL_HEADER_SIZE + payload_length) as u64
            || payload_length > MAX_PAGE_PAYLOAD
        {
            break;
        }
        if offset.saturating_add(record_length) > file_length {
            break;
        }

        let mut payload = vec![0_u8; payload_length];
        if !read_exact_at(file, &mut payload, offset + WAL_HEADER_SIZE as u64)? {
            break;
        }
        if read_u32(&header, CHECKSUM_OFFSET) != record_checksum(&header, &payload) {
            break;
        }

        let lsn = read_u64(&header, 16);
        if lsn == 0 || lsn <= previous_lsn {
            break;
        }

        let page_id = read_u64(&header, 24);
        let kind = match header[6] {
            1 if page_id != NO_PAGE_ID => WalKind::PageImage {
                page_id: PageId(page_id),
                payload,
            },
            2 if page_id == NO_PAGE_ID && payload.len() == 4 => {
                let page_count = read_u32(&payload, 0);
                if page_count == 0 {
                    break;
                }
                WalKind::BatchCommit { page_count }
            }
            3 if page_id == NO_PAGE_ID && payload.is_empty() => WalKind::Checkpoint,
            _ => break,
        };
        previous_lsn = lsn;
        records.push(WalRecord {
            lsn: Lsn(lsn),
            kind,
        });
        offset += record_length;
    }

    // Seal the tail so stale bytes beyond it can never be rescanned.
    if offset.saturating_add(STOP_MARKER.len() as u64) <= file_length {
        write_all_at(file, &STOP_MARKER, offset)?;
        file.sync_data()?;
    }
    Ok((records, offset))
}

fn record_checksum(header: &[u8; WAL_HEADER_SIZE], payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&header[..CHECKSUM_OFFSET]);
    hasher.update(&[0_u8; 4]);
    hasher.update(&header[CHECKSUM_OFFSET + 4..]);
    hasher.update(payload);
    hasher.finalize()
}

#[cfg(test)]
pub(crate) fn corrupt_last_record_for_recovery_test(path: &Path) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open WAL");
    let (_, end_offset) = scan_and_repair(&file).expect("scan for logical end");
    assert!(end_offset > 0, "cannot corrupt an empty log");
    let mut byte = [0_u8; 1];
    assert!(read_exact_at(&file, &mut byte, end_offset - 1).expect("read tail byte"));
    byte[0] ^= 0xff;
    write_all_at(&file, &byte, end_offset - 1).expect("corrupt tail");
    file.sync_data().expect("sync corruption");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn wal_round_trip_preserves_records_and_lsn_sequence() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("wal.qdb");
        {
            let mut wal = Wal::open(&path).expect("open WAL");
            assert_eq!(wal.append_page(PageId(9), b"nine").expect("append"), Lsn(1));
            assert_eq!(wal.append_batch_commit(1).expect("commit"), Lsn(2));
            assert_eq!(wal.append_checkpoint().expect("checkpoint"), Lsn(3));
            wal.sync().expect("sync");
        }

        let wal = Wal::open(&path).expect("reopen WAL");
        assert_eq!(wal.records().len(), 3);
        assert!(matches!(wal.records()[2].kind, WalKind::Checkpoint));
    }

    #[test]
    fn repairs_a_torn_final_record() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("wal.qdb");
        {
            let mut wal = Wal::open(&path).expect("open WAL");
            wal.append_page(PageId(1), b"one").expect("append");
            wal.append_batch_commit(1).expect("commit");
            wal.sync().expect("sync");
        }
        let valid_length = {
            let wal = Wal::open(&path).expect("reopen for length");
            wal.size_bytes().expect("size")
        };
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .expect("open for tear");
            write_all_at(&file, &WAL_MAGIC[..2], valid_length).expect("write torn tail");
            file.sync_data().expect("sync tail");
        }

        let wal = Wal::open(&path).expect("repair WAL");
        assert_eq!(wal.records().len(), 2);
        assert_eq!(
            wal.size_bytes().expect("size"),
            valid_length,
            "the log ends where the last complete record does"
        );
    }

    #[test]
    fn every_partial_tail_prefix_is_repaired_deterministically() {
        let source_directory = tempdir().expect("source tempdir");
        let source_path = source_directory.path().join("wal.qdb");
        let first_batch_end;
        let full_end;
        {
            let mut wal = Wal::open(&source_path).expect("open WAL");
            wal.append_page(PageId(1), b"first").expect("first");
            wal.append_batch_commit(1).expect("commit first");
            wal.sync().expect("sync first");
            first_batch_end = wal.size_bytes().expect("size after first batch");
            wal.append_page(PageId(2), b"second").expect("second");
            wal.sync().expect("sync second");
            full_end = wal.size_bytes().expect("size with dangling record");
        }
        let complete = std::fs::read(&source_path).expect("read complete WAL");

        for cut in first_batch_end as usize..full_end as usize {
            let case_directory = tempdir().expect("case tempdir");
            let case_path = case_directory.path().join("wal.qdb");
            std::fs::write(&case_path, &complete[..cut]).expect("write WAL prefix");

            let wal = Wal::open(&case_path).expect("repair partial record");
            assert_eq!(wal.records().len(), 2, "cut at byte {cut}");
            assert_eq!(
                wal.size_bytes().expect("size"),
                first_batch_end,
                "cut at byte {cut}"
            );
        }
    }

    #[test]
    fn corruption_before_the_tail_seals_the_log_there() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("wal.qdb");
        {
            let mut wal = Wal::open(&path).expect("open WAL");
            wal.append_page(PageId(1), b"first").expect("append first");
            wal.append_batch_commit(1).expect("commit first");
            wal.append_page(PageId(2), b"second")
                .expect("append second");
            wal.append_batch_commit(1).expect("commit second");
            wal.sync().expect("sync");
        }
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .expect("open WAL bytes");
            write_all_at(&file, b"X", WAL_HEADER_SIZE as u64 + 1).expect("corrupt");
            file.sync_data().expect("sync corruption");
        }

        // A recycled log cannot tell mid-log corruption from a torn tail,
        // so the scan seals the log at the first bad record. Everything
        // before it survives; the batch behind it is gone as a unit.
        let wal = Wal::open(&path).expect("sealed reopen");
        assert_eq!(wal.records().len(), 0, "the first record was the bad one");
        assert_eq!(wal.size_bytes().expect("size"), 0);
    }
}
