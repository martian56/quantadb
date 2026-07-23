use crate::{Result, Timestamp, TransactionError};
use quantadb_index::IndexRoot;
use quantadb_storage::PageId;

const MAGIC: [u8; 4] = *b"QNIR";
const FORMAT_VERSION: u16 = 1;
const ENCODED_SIZE: usize = 48;
const FLAG_HAS_ROOT: u16 = 1;
const NO_PAGE: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexGeneration {
    pub(crate) timestamp: Timestamp,
    pub(crate) root: Option<IndexRoot>,
    pub(crate) manifest_page_id: PageId,
}

impl IndexGeneration {
    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = vec![0_u8; ENCODED_SIZE];
        bytes[0..4].copy_from_slice(&MAGIC);
        write_u16(&mut bytes, 4, FORMAT_VERSION);
        if self.root.is_some() {
            write_u16(&mut bytes, 6, FLAG_HAS_ROOT);
        }
        write_u64(&mut bytes, 8, self.timestamp.0);
        write_u64(
            &mut bytes,
            16,
            self.root.map_or(NO_PAGE, |root| root.page_id.0),
        );
        write_u16(&mut bytes, 24, self.root.map_or(0, |root| root.height));
        write_u64(&mut bytes, 32, self.root.map_or(0, |root| root.entries));
        bytes
    }

    pub(crate) fn decode(page_id: PageId, bytes: &[u8]) -> Result<Option<Self>> {
        if bytes.len() < 4 || bytes[0..4] != MAGIC {
            return Ok(None);
        }
        if bytes.len() != ENCODED_SIZE {
            return Err(corrupt(page_id, "manifest length is not 48 bytes"));
        }
        let version = read_u16(bytes, 4);
        if version != FORMAT_VERSION {
            return Err(corrupt(
                page_id,
                format!("unsupported index manifest version {version}"),
            ));
        }
        let flags = read_u16(bytes, 6);
        if flags & !FLAG_HAS_ROOT != 0 {
            return Err(corrupt(
                page_id,
                format!("unknown manifest flags {flags:#06x}"),
            ));
        }
        if bytes[26..32].iter().any(|byte| *byte != 0)
            || bytes[40..48].iter().any(|byte| *byte != 0)
        {
            return Err(corrupt(page_id, "reserved manifest bytes are not zero"));
        }
        let timestamp = Timestamp(read_u64(bytes, 8));
        let root_page_id = read_u64(bytes, 16);
        let height = read_u16(bytes, 24);
        let entries = read_u64(bytes, 32);
        let root = if flags & FLAG_HAS_ROOT != 0 {
            if root_page_id == NO_PAGE || height == 0 || entries == 0 {
                return Err(corrupt(page_id, "root metadata is incomplete"));
            }
            Some(IndexRoot {
                page_id: PageId(root_page_id),
                height,
                entries,
            })
        } else {
            if root_page_id != NO_PAGE || height != 0 || entries != 0 {
                return Err(corrupt(page_id, "empty generation contains root metadata"));
            }
            None
        };
        Ok(Some(Self {
            timestamp,
            root,
            manifest_page_id: page_id,
        }))
    }
}

fn corrupt(page_id: PageId, reason: impl Into<String>) -> TransactionError {
    TransactionError::CorruptRecord {
        page_id,
        reason: reason.into(),
    }
}

fn write_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifests_round_trip_empty_and_populated_generations() {
        for root in [
            None,
            Some(IndexRoot {
                page_id: PageId(12),
                height: 3,
                entries: 99,
            }),
        ] {
            let generation = IndexGeneration {
                timestamp: Timestamp(7),
                root,
                manifest_page_id: PageId(20),
            };
            assert_eq!(
                IndexGeneration::decode(PageId(20), &generation.encode())
                    .expect("decode")
                    .expect("manifest"),
                generation
            );
        }
    }
}
