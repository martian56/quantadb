use crate::{Result, Timestamp, TransactionError};
use quantadb_storage::{PageId, MAX_PAGE_PAYLOAD};

const MAGIC: [u8; 4] = *b"QNMV";
const FORMAT_VERSION: u16 = 1;
const HEADER_SIZE: usize = 32;
const FLAG_TOMBSTONE: u16 = 1;
const RESERVED_OFFSET: usize = 24;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionRecord {
    pub(crate) timestamp: Timestamp,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Option<Vec<u8>>,
}

impl VersionRecord {
    pub(crate) fn validate_size(key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let value_length = value.map_or(0, <[u8]>::len);
        let actual = HEADER_SIZE
            .checked_add(key.len())
            .and_then(|length| length.checked_add(value_length))
            .ok_or(TransactionError::RecordTooLarge {
                actual: usize::MAX,
                maximum: MAX_PAGE_PAYLOAD,
            })?;
        if actual > MAX_PAGE_PAYLOAD {
            return Err(TransactionError::RecordTooLarge {
                actual,
                maximum: MAX_PAGE_PAYLOAD,
            });
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        Self::validate_size(&self.key, self.value.as_deref())?;
        let mut bytes =
            vec![0_u8; HEADER_SIZE + self.key.len() + self.value.as_ref().map_or(0, Vec::len)];
        bytes[0..4].copy_from_slice(&MAGIC);
        write_u16(&mut bytes, 4, FORMAT_VERSION);
        if self.value.is_none() {
            write_u16(&mut bytes, 6, FLAG_TOMBSTONE);
        }
        write_u64(&mut bytes, 8, self.timestamp.0);
        write_u32(&mut bytes, 16, self.key.len() as u32);
        write_u32(
            &mut bytes,
            20,
            self.value.as_ref().map_or(0, Vec::len) as u32,
        );
        bytes[HEADER_SIZE..HEADER_SIZE + self.key.len()].copy_from_slice(&self.key);
        if let Some(value) = &self.value {
            bytes[HEADER_SIZE + self.key.len()..].copy_from_slice(value);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(page_id: PageId, bytes: &[u8]) -> Result<Option<Self>> {
        if bytes.len() < 4 || bytes[0..4] != MAGIC {
            return Ok(None);
        }
        if bytes.len() < HEADER_SIZE {
            return Err(corrupt(page_id, "record is shorter than its header"));
        }
        let version = read_u16(bytes, 4);
        if version != FORMAT_VERSION {
            return Err(corrupt(
                page_id,
                format!("unsupported record format version {version}"),
            ));
        }
        let flags = read_u16(bytes, 6);
        if flags & !FLAG_TOMBSTONE != 0 {
            return Err(corrupt(page_id, format!("unknown flags {flags:#06x}")));
        }
        if bytes[RESERVED_OFFSET..HEADER_SIZE]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(corrupt(page_id, "reserved header bytes are not zero"));
        }
        let timestamp = read_u64(bytes, 8);
        if timestamp == 0 {
            return Err(corrupt(page_id, "commit timestamp cannot be zero"));
        }
        let key_length = read_u32(bytes, 16) as usize;
        let value_length = read_u32(bytes, 20) as usize;
        let expected_length = HEADER_SIZE
            .checked_add(key_length)
            .and_then(|length| length.checked_add(value_length))
            .ok_or_else(|| corrupt(page_id, "record length overflow"))?;
        if expected_length != bytes.len() {
            return Err(corrupt(
                page_id,
                format!(
                    "record length mismatch: header declares {expected_length}, page contains {}",
                    bytes.len()
                ),
            ));
        }
        let tombstone = flags & FLAG_TOMBSTONE != 0;
        if tombstone && value_length != 0 {
            return Err(corrupt(page_id, "tombstone contains a value"));
        }
        let key = bytes[HEADER_SIZE..HEADER_SIZE + key_length].to_vec();
        let value = (!tombstone).then(|| bytes[HEADER_SIZE + key_length..].to_vec());
        Ok(Some(Self {
            timestamp: Timestamp(timestamp),
            key,
            value,
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

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
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
    fn version_record_round_trips_values_and_tombstones() {
        for value in [Some(b"value".to_vec()), None] {
            let record = VersionRecord {
                timestamp: Timestamp(9),
                key: b"key".to_vec(),
                value,
            };
            assert_eq!(
                VersionRecord::decode(PageId(4), &record.encode().expect("encode"))
                    .expect("decode"),
                Some(record)
            );
        }
    }

    #[test]
    fn version_record_rejects_oversized_values() {
        let key = b"key";
        let value = vec![0_u8; MAX_PAGE_PAYLOAD];
        assert!(matches!(
            VersionRecord::validate_size(key, Some(&value)),
            Err(TransactionError::RecordTooLarge { .. })
        ));
    }

    #[test]
    fn version_record_rejects_nonzero_reserved_bytes() {
        let record = VersionRecord {
            timestamp: Timestamp(1),
            key: b"key".to_vec(),
            value: Some(b"value".to_vec()),
        };
        let mut encoded = record.encode().expect("encode");
        encoded[RESERVED_OFFSET] = 1;
        assert!(matches!(
            VersionRecord::decode(PageId(8), &encoded),
            Err(TransactionError::CorruptRecord {
                page_id: PageId(8),
                ..
            })
        ));
    }
}
