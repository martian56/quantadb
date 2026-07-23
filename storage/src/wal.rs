use crate::{
    page::{read_u16, read_u32, read_u64, write_u16, write_u32, write_u64},
    Lsn, PageId, Result, StorageError, MAX_PAGE_PAYLOAD,
};
use crc32fast::Hasher;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

const WAL_MAGIC: [u8; 4] = *b"QNWL";
const WAL_FORMAT_VERSION: u16 = 1;
const WAL_HEADER_SIZE: usize = 40;
const CHECKSUM_OFFSET: usize = 32;
const NO_PAGE_ID: u64 = u64::MAX;

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
}

impl Wal {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        let records = scan_and_repair(&mut file)?;
        let next_lsn = records
            .last()
            .map_or(1, |record| record.lsn.0.saturating_add(1));
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            records,
            next_lsn,
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

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&header)?;
        self.file.write_all(&payload)?;
        self.records.push(WalRecord { lsn, kind });
        Ok(lsn)
    }
}

fn scan_and_repair(file: &mut File) -> Result<Vec<WalRecord>> {
    let file_length = file.metadata()?.len();
    let mut records = Vec::new();
    let mut offset = 0_u64;
    let mut previous_lsn = 0_u64;

    while offset < file_length {
        let remaining = file_length - offset;
        if remaining < WAL_HEADER_SIZE as u64 {
            file.set_len(offset)?;
            break;
        }

        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0_u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header)?;

        if header[0..4] != WAL_MAGIC {
            return Err(StorageError::CorruptWal {
                offset,
                reason: "invalid record magic".to_owned(),
            });
        }
        let version = read_u16(&header, 4);
        if version != WAL_FORMAT_VERSION {
            return Err(StorageError::CorruptWal {
                offset,
                reason: format!("unsupported WAL format version {version}"),
            });
        }

        let record_length = u64::from(read_u32(&header, 8));
        let payload_length = read_u32(&header, 12) as usize;
        if record_length != (WAL_HEADER_SIZE + payload_length) as u64
            || payload_length > MAX_PAGE_PAYLOAD
        {
            return Err(StorageError::CorruptWal {
                offset,
                reason: "invalid record length".to_owned(),
            });
        }
        if offset.saturating_add(record_length) > file_length {
            file.set_len(offset)?;
            break;
        }

        let mut payload = vec![0_u8; payload_length];
        file.read_exact(&mut payload)?;
        let stored_checksum = read_u32(&header, CHECKSUM_OFFSET);
        let actual_checksum = record_checksum(&header, &payload);
        if stored_checksum != actual_checksum {
            if offset + record_length == file_length {
                file.set_len(offset)?;
                break;
            }
            return Err(StorageError::CorruptWal {
                offset,
                reason: "record checksum mismatch".to_owned(),
            });
        }

        let lsn = read_u64(&header, 16);
        if lsn == 0 || lsn <= previous_lsn {
            return Err(StorageError::CorruptWal {
                offset,
                reason: format!("non-monotonic LSN {lsn} after {previous_lsn}"),
            });
        }
        previous_lsn = lsn;

        let page_id = read_u64(&header, 24);
        let kind = match header[6] {
            1 if page_id != NO_PAGE_ID => WalKind::PageImage {
                page_id: PageId(page_id),
                payload,
            },
            2 if page_id == NO_PAGE_ID && payload.len() == 4 => {
                let page_count = read_u32(&payload, 0);
                if page_count == 0 {
                    return Err(StorageError::CorruptWal {
                        offset,
                        reason: "batch commit cannot be empty".to_owned(),
                    });
                }
                WalKind::BatchCommit { page_count }
            }
            3 if page_id == NO_PAGE_ID && payload.is_empty() => WalKind::Checkpoint,
            record_type => {
                return Err(StorageError::CorruptWal {
                    offset,
                    reason: format!("invalid record type {record_type}"),
                });
            }
        };
        records.push(WalRecord {
            lsn: Lsn(lsn),
            kind,
        });
        offset += record_length;
    }

    file.seek(SeekFrom::End(0))?;
    Ok(records)
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
    let length = std::fs::metadata(path).expect("metadata").len();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open WAL");
    file.seek(SeekFrom::Start(length - 1)).expect("seek tail");
    file.write_all(b"X").expect("corrupt tail");
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
        let valid_length = std::fs::metadata(&path).expect("metadata").len();
        {
            let mut file = OpenOptions::new().append(true).open(&path).expect("append");
            file.write_all(&WAL_MAGIC[..2]).expect("write torn tail");
            file.sync_data().expect("sync tail");
        }

        let wal = Wal::open(&path).expect("repair WAL");
        assert_eq!(wal.records().len(), 2);
        assert_eq!(
            std::fs::metadata(&path).expect("metadata").len(),
            valid_length
        );
    }

    #[test]
    fn every_partial_tail_prefix_is_repaired_deterministically() {
        let source_directory = tempdir().expect("source tempdir");
        let source_path = source_directory.path().join("wal.qdb");
        let first_record_length;
        {
            let mut wal = Wal::open(&source_path).expect("open WAL");
            wal.append_page(PageId(1), b"first").expect("first");
            wal.append_batch_commit(1).expect("commit first");
            wal.sync().expect("sync first");
            first_record_length = std::fs::metadata(&source_path)
                .expect("first metadata")
                .len();
            wal.append_page(PageId(2), b"second").expect("second");
        }
        let complete = std::fs::read(&source_path).expect("read complete WAL");

        for cut in first_record_length as usize..complete.len() {
            let case_directory = tempdir().expect("case tempdir");
            let case_path = case_directory.path().join("wal.qdb");
            std::fs::write(&case_path, &complete[..cut]).expect("write WAL prefix");

            let wal = Wal::open(&case_path).expect("repair partial record");
            assert_eq!(wal.records().len(), 2, "cut at byte {cut}");
            assert_eq!(
                std::fs::metadata(&case_path).expect("metadata").len(),
                first_record_length,
                "cut at byte {cut}"
            );
        }
    }

    #[test]
    fn rejects_checksum_corruption_before_the_wal_tail() {
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
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .expect("open WAL bytes");
            file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64 + 1))
                .expect("seek");
            file.write_all(b"X").expect("corrupt");
            file.sync_data().expect("sync corruption");
        }

        assert!(matches!(
            Wal::open(&path),
            Err(StorageError::CorruptWal { .. })
        ));
    }
}
