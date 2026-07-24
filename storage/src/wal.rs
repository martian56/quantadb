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
/// Each segment file is preallocated in chunks of this size and recycled
/// in place, so the routine commit sync never touches file metadata.
/// Growing a file on every append makes each sync journal a size change,
/// which measured six times slower than syncing data blocks alone.
const WAL_PREALLOCATE_BYTES: u64 = 64 << 20;
/// How far ahead of the append position a segment is zero-filled.
///
/// Reserving length with set_len leaves a sparse file, and writing into a
/// hole still allocates blocks, which journals metadata on the next sync,
/// the very cost preallocation exists to avoid. Zero-filling genuinely
/// allocates, so it happens in windows this size ahead of the log's end:
/// one bulk write every few thousand records instead of an allocation on
/// every commit.
const WAL_ZERO_FILL_AHEAD: u64 = 4 << 20;
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

struct Segment {
    file: File,
    end_offset: u64,
    file_length: u64,
    allocated_offset: u64,
}

impl Segment {
    fn open(path: &Path) -> Result<(Self, Vec<WalRecord>)> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        let (records, end_offset) = scan_and_repair(&file)?;
        let mut file_length = file.metadata()?.len();
        if file_length < WAL_PREALLOCATE_BYTES {
            // Preallocation is sparse and instant; the payoff is that the
            // per-commit sync never journals a size change again.
            file.set_len(WAL_PREALLOCATE_BYTES)?;
            file.sync_all()?;
            file_length = WAL_PREALLOCATE_BYTES;
        }
        Ok((
            Self {
                file,
                end_offset,
                file_length,
                // Blocks up to the scanned end were written before; anything
                // beyond gets zero-filled ahead of use.
                allocated_offset: end_offset,
            },
            records,
        ))
    }

    fn append_record(&mut self, header: &[u8; WAL_HEADER_SIZE], payload: &[u8]) -> Result<()> {
        let record_length = (WAL_HEADER_SIZE + payload.len()) as u64;
        let record_end =
            self.end_offset
                .checked_add(record_length)
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
        if needed > self.allocated_offset {
            let target = needed
                .saturating_add(WAL_ZERO_FILL_AHEAD)
                .min(self.file_length);
            zero_fill(&self.file, self.allocated_offset, target)?;
            self.allocated_offset = target;
        }

        write_all_at(&self.file, header, self.end_offset)?;
        write_all_at(
            &self.file,
            payload,
            self.end_offset + WAL_HEADER_SIZE as u64,
        )?;
        write_all_at(&self.file, &STOP_MARKER, record_end)?;
        self.end_offset = record_end;
        Ok(())
    }

    fn seal_and_sync(&mut self) -> Result<()> {
        seal_file(&self.file)?;
        self.end_offset = 0;
        Ok(())
    }
}

/// Two alternating segment files. Appends go to the active one; a rotation
/// swaps in the empty spare so recycling the old segment can happen off the
/// commit path. Recovery scans both and orders them by first LSN.
pub(crate) struct Wal {
    segments: [Segment; 2],
    active: usize,
    retired: Option<usize>,
    records: Vec<WalRecord>,
    next_lsn: u64,
}

impl Wal {
    pub(crate) fn open(primary: &Path, secondary: &Path) -> Result<Self> {
        let (segment_a, records_a) = Segment::open(primary)?;
        let (segment_b, records_b) = Segment::open(secondary)?;

        // A non-monotonic seam cannot come from any crash ordering this
        // module produces, so the later file is sealed as garbage.
        let mut seal_garbage = None;
        let (records, active) = match (records_a.is_empty(), records_b.is_empty()) {
            (_, true) => (records_a, 0),
            (true, false) => (records_b, 1),
            (false, false) => {
                let (mut earlier, later, later_index) = if records_a[0].lsn < records_b[0].lsn {
                    (records_a, records_b, 1_usize)
                } else {
                    (records_b, records_a, 0_usize)
                };
                let seam_is_monotonic = earlier.last().is_none_or(|last| later[0].lsn > last.lsn);
                if seam_is_monotonic {
                    earlier.extend(later);
                    (earlier, later_index)
                } else {
                    seal_garbage = Some(later_index);
                    (earlier, 1 - later_index)
                }
            }
        };

        let next_lsn = records
            .last()
            .map_or(1, |record| record.lsn.0.saturating_add(1));
        let mut wal = Self {
            segments: [segment_a, segment_b],
            active,
            retired: None,
            records,
            next_lsn,
        };
        if let Some(index) = seal_garbage {
            wal.segments[index].seal_and_sync()?;
        }
        Ok(wal)
    }

    /// Only true after a crash between a rotation and its recycle.
    pub(crate) fn both_segments_live(&self) -> bool {
        self.segments[0].end_offset > 0 && self.segments[1].end_offset > 0
    }

    pub(crate) fn can_rotate(&self) -> bool {
        self.retired.is_none() && self.segments[1 - self.active].end_offset == 0
    }

    /// Returns the boundary LSN: records below it live in the retired segment.
    pub(crate) fn rotate(&mut self) -> Result<u64> {
        if !self.can_rotate() {
            return Err(StorageError::CorruptWal {
                offset: 0,
                reason: "log rotation needs an empty spare segment".to_owned(),
            });
        }
        self.retired = Some(self.active);
        self.active = 1 - self.active;
        Ok(self.next_lsn)
    }

    pub(crate) fn retired_file(&self) -> Result<File> {
        let retired = self.retired.ok_or_else(|| StorageError::CorruptWal {
            offset: 0,
            reason: "no retired segment to recycle".to_owned(),
        })?;
        Ok(self.segments[retired].file.try_clone()?)
    }

    pub(crate) fn finish_recycle(&mut self, boundary_lsn: u64) -> Result<()> {
        let retired = self
            .retired
            .take()
            .ok_or_else(|| StorageError::CorruptWal {
                offset: 0,
                reason: "no retired segment to recycle".to_owned(),
            })?;
        self.segments[retired].end_offset = 0;
        self.records.retain(|record| record.lsn.0 >= boundary_lsn);
        Ok(())
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
                offset: self.segments[self.active].end_offset,
                reason: "batch commit cannot be empty".to_owned(),
            });
        }
        let page_count = u32::try_from(page_count).map_err(|_| StorageError::CorruptWal {
            offset: self.segments[self.active].end_offset,
            reason: "batch page count exceeds u32".to_owned(),
        })?;
        self.append(WalKind::BatchCommit { page_count })
    }

    pub(crate) fn sync(&self) -> Result<()> {
        self.segments[self.active].file.sync_data()?;
        Ok(())
    }

    pub(crate) fn records(&self) -> &[WalRecord] {
        &self.records
    }

    pub(crate) fn size_bytes(&self) -> Result<u64> {
        Ok(self.segments[self.active].end_offset)
    }

    /// Recycles both segments in place and clears any pending recycle,
    /// so an explicit checkpoint also repairs a failed background one.
    pub(crate) fn reset_after_checkpoint(&mut self) -> Result<()> {
        for segment in &mut self.segments {
            if segment.end_offset > 0 {
                segment.seal_and_sync()?;
            }
        }
        self.retired = None;
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
                offset: self.segments[self.active].end_offset,
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
                offset: self.segments[self.active].end_offset,
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
                    offset: self.segments[self.active].end_offset,
                    reason: "record length overflow".to_owned(),
                })?;
        let record_length_u32 =
            u32::try_from(record_length).map_err(|_| StorageError::CorruptWal {
                offset: self.segments[self.active].end_offset,
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

        self.segments[self.active].append_record(&header, &payload)?;
        self.records.push(WalRecord { lsn, kind });
        Ok(lsn)
    }
}

/// A stop marker at offset zero makes the file scan as empty.
pub(crate) fn seal_file(file: &File) -> Result<()> {
    write_all_at(file, &STOP_MARKER, 0)?;
    file.sync_data()?;
    Ok(())
}

/// Write real zeros so the blocks are allocated, not holes.
fn zero_fill(file: &File, from: u64, to: u64) -> Result<()> {
    const CHUNK: usize = 64 << 10;
    let zeros = [0_u8; CHUNK];
    let mut offset = from;
    while offset < to {
        let step = ((to - offset) as usize).min(CHUNK);
        write_all_at(file, &zeros[..step], offset)?;
        offset += step as u64;
    }
    Ok(())
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

/// Scan a segment from the start, stopping at the first thing that is not
/// a complete, checksummed, monotonic record.
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

    fn wal_paths(directory: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
        (directory.join("wal.qdb"), directory.join("wal2.qdb"))
    }

    #[test]
    fn wal_round_trip_preserves_records_and_lsn_sequence() {
        let directory = tempdir().expect("tempdir");
        let (primary, secondary) = wal_paths(directory.path());
        {
            let mut wal = Wal::open(&primary, &secondary).expect("open WAL");
            assert_eq!(wal.append_page(PageId(9), b"nine").expect("append"), Lsn(1));
            assert_eq!(wal.append_batch_commit(1).expect("commit"), Lsn(2));
            assert_eq!(wal.append_checkpoint().expect("checkpoint"), Lsn(3));
            wal.sync().expect("sync");
        }

        let wal = Wal::open(&primary, &secondary).expect("reopen WAL");
        assert_eq!(wal.records().len(), 3);
        assert!(matches!(wal.records()[2].kind, WalKind::Checkpoint));
    }

    #[test]
    fn repairs_a_torn_final_record() {
        let directory = tempdir().expect("tempdir");
        let (primary, secondary) = wal_paths(directory.path());
        {
            let mut wal = Wal::open(&primary, &secondary).expect("open WAL");
            wal.append_page(PageId(1), b"one").expect("append");
            wal.append_batch_commit(1).expect("commit");
            wal.sync().expect("sync");
        }
        let valid_length = {
            let wal = Wal::open(&primary, &secondary).expect("reopen for length");
            wal.size_bytes().expect("size")
        };
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&primary)
                .expect("open for tear");
            write_all_at(&file, &WAL_MAGIC[..2], valid_length).expect("write torn tail");
            file.sync_data().expect("sync tail");
        }

        let wal = Wal::open(&primary, &secondary).expect("repair WAL");
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
        let (source_primary, source_secondary) = wal_paths(source_directory.path());
        let first_batch_end;
        let full_end;
        {
            let mut wal = Wal::open(&source_primary, &source_secondary).expect("open WAL");
            wal.append_page(PageId(1), b"first").expect("first");
            wal.append_batch_commit(1).expect("commit first");
            wal.sync().expect("sync first");
            first_batch_end = wal.size_bytes().expect("size after first batch");
            wal.append_page(PageId(2), b"second").expect("second");
            wal.sync().expect("sync second");
            full_end = wal.size_bytes().expect("size with dangling record");
        }
        let complete = std::fs::read(&source_primary).expect("read complete WAL");

        for cut in first_batch_end as usize..full_end as usize {
            let case_directory = tempdir().expect("case tempdir");
            let (case_primary, case_secondary) = wal_paths(case_directory.path());
            std::fs::write(&case_primary, &complete[..cut]).expect("write WAL prefix");

            let wal = Wal::open(&case_primary, &case_secondary).expect("repair partial record");
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
        let (primary, secondary) = wal_paths(directory.path());
        {
            let mut wal = Wal::open(&primary, &secondary).expect("open WAL");
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
                .open(&primary)
                .expect("open WAL bytes");
            write_all_at(&file, b"X", WAL_HEADER_SIZE as u64 + 1).expect("corrupt");
            file.sync_data().expect("sync corruption");
        }

        // A recycled log cannot tell mid-log corruption from a torn tail,
        // so the scan seals the log at the first bad record. Everything
        // before it survives; the batch behind it is gone as a unit.
        let wal = Wal::open(&primary, &secondary).expect("sealed reopen");
        assert_eq!(wal.records().len(), 0, "the first record was the bad one");
        assert_eq!(wal.size_bytes().expect("size"), 0);
    }

    #[test]
    fn rotation_moves_appends_to_the_spare_segment() {
        let directory = tempdir().expect("tempdir");
        let (primary, secondary) = wal_paths(directory.path());
        let mut wal = Wal::open(&primary, &secondary).expect("open WAL");
        wal.append_page(PageId(1), b"before").expect("append");
        wal.append_batch_commit(1).expect("commit");
        wal.sync().expect("sync");
        let retired_size = wal.size_bytes().expect("size");

        assert!(wal.can_rotate());
        let boundary = wal.rotate().expect("rotate");
        assert_eq!(boundary, 3, "next LSN at rotation time");
        assert!(!wal.can_rotate(), "recycle is still pending");
        assert_eq!(wal.size_bytes().expect("size"), 0, "fresh active segment");

        wal.append_page(PageId(2), b"after").expect("append after");
        wal.append_batch_commit(1).expect("commit after");
        wal.sync().expect("sync after");

        seal_file(&wal.retired_file().expect("retired handle")).expect("seal retired");
        wal.finish_recycle(boundary).expect("finish recycle");
        assert!(wal.can_rotate(), "spare is empty again");
        assert_eq!(
            wal.records().len(),
            2,
            "retired records left memory with the recycle"
        );
        assert!(retired_size > 0);
    }

    #[test]
    fn crash_between_rotation_and_recycle_replays_both_segments_in_order() {
        let directory = tempdir().expect("tempdir");
        let (primary, secondary) = wal_paths(directory.path());
        {
            let mut wal = Wal::open(&primary, &secondary).expect("open WAL");
            wal.append_page(PageId(1), b"old").expect("append old");
            wal.append_batch_commit(1).expect("commit old");
            wal.sync().expect("sync old");
            let _boundary = wal.rotate().expect("rotate");
            wal.append_page(PageId(2), b"new").expect("append new");
            wal.append_batch_commit(1).expect("commit new");
            wal.sync().expect("sync new");
            // Dropped here without finish_recycle: the crash window.
        }

        let wal = Wal::open(&primary, &secondary).expect("reopen both live");
        assert!(wal.both_segments_live());
        let lsns: Vec<u64> = wal.records().iter().map(|record| record.lsn.0).collect();
        assert_eq!(lsns, vec![1, 2, 3, 4], "both segments, in LSN order");
        assert!(
            !wal.can_rotate(),
            "no rotation until a checkpoint clears it"
        );
    }

    #[test]
    fn a_recycled_segment_is_reused_from_its_start() {
        let directory = tempdir().expect("tempdir");
        let (primary, secondary) = wal_paths(directory.path());
        let mut wal = Wal::open(&primary, &secondary).expect("open WAL");

        for lap in 0..3_u64 {
            wal.append_page(PageId(lap), &lap.to_le_bytes())
                .expect("append");
            wal.append_batch_commit(1).expect("commit");
            wal.sync().expect("sync");
            let boundary = wal.rotate().expect("rotate");
            seal_file(&wal.retired_file().expect("retired")).expect("seal");
            wal.finish_recycle(boundary).expect("recycle");
        }

        wal.append_page(PageId(9), b"final").expect("append final");
        wal.append_batch_commit(1).expect("commit final");
        wal.sync().expect("sync final");
        drop(wal);

        let wal = Wal::open(&primary, &secondary).expect("reopen");
        let lsns: Vec<u64> = wal.records().iter().map(|record| record.lsn.0).collect();
        assert_eq!(lsns, vec![7, 8], "only the live lap survives on disk");
    }

    #[test]
    fn classic_reset_empties_both_segments_and_clears_a_pending_recycle() {
        let directory = tempdir().expect("tempdir");
        let (primary, secondary) = wal_paths(directory.path());
        let mut wal = Wal::open(&primary, &secondary).expect("open WAL");
        wal.append_page(PageId(1), b"a").expect("append");
        wal.append_batch_commit(1).expect("commit");
        wal.rotate().expect("rotate");
        wal.append_page(PageId(2), b"b").expect("append b");
        wal.append_batch_commit(1).expect("commit b");
        wal.sync().expect("sync");

        wal.reset_after_checkpoint().expect("reset");
        assert!(wal.can_rotate(), "reset cleared the pending recycle");
        assert_eq!(wal.records().len(), 0);
        assert!(!wal.both_segments_live());

        wal.append_page(PageId(3), b"c")
            .expect("append after reset");
        assert_eq!(wal.records().len(), 1);
    }
}
