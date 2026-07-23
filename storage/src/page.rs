use crate::{Result, StorageError};
use crc32fast::Hasher;
use std::fmt;

pub const PAGE_SIZE: usize = 8 * 1024;
const PAGE_HEADER_SIZE: usize = 64;
pub const MAX_PAGE_PAYLOAD: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

const PAGE_MAGIC: [u8; 4] = *b"QNPG";
const PAGE_FORMAT_VERSION: u16 = 1;
const MAGIC_OFFSET: usize = 0;
const VERSION_OFFSET: usize = 4;
const PAGE_ID_OFFSET: usize = 8;
const LSN_OFFSET: usize = 16;
const PAYLOAD_LENGTH_OFFSET: usize = 24;
const CHECKSUM_OFFSET: usize = 28;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(pub u64);

impl fmt::Display for PageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    id: PageId,
    lsn: Lsn,
    payload: Vec<u8>,
}

impl Page {
    pub fn new(id: PageId, payload: impl Into<Vec<u8>>) -> Result<Self> {
        Self::with_lsn(id, Lsn(0), payload)
    }

    pub(crate) fn with_lsn(id: PageId, lsn: Lsn, payload: impl Into<Vec<u8>>) -> Result<Self> {
        let payload = payload.into();
        if payload.len() > MAX_PAGE_PAYLOAD {
            return Err(StorageError::PageTooLarge {
                actual: payload.len(),
                maximum: MAX_PAGE_PAYLOAD,
            });
        }
        Ok(Self { id, lsn, payload })
    }

    #[must_use]
    pub const fn id(&self) -> PageId {
        self.id
    }

    #[must_use]
    pub const fn lsn(&self) -> Lsn {
        self.lsn
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub(crate) fn encode(&self) -> [u8; PAGE_SIZE] {
        let mut bytes = [0_u8; PAGE_SIZE];
        bytes[MAGIC_OFFSET..MAGIC_OFFSET + 4].copy_from_slice(&PAGE_MAGIC);
        write_u16(&mut bytes, VERSION_OFFSET, PAGE_FORMAT_VERSION);
        write_u64(&mut bytes, PAGE_ID_OFFSET, self.id.0);
        write_u64(&mut bytes, LSN_OFFSET, self.lsn.0);
        write_u32(&mut bytes, PAYLOAD_LENGTH_OFFSET, self.payload.len() as u32);
        bytes[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + self.payload.len()]
            .copy_from_slice(&self.payload);

        let checksum = checksum_with_zeroed_field(&bytes);
        write_u32(&mut bytes, CHECKSUM_OFFSET, checksum);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(StorageError::InvalidPageLength {
                actual: bytes.len(),
                expected: PAGE_SIZE,
            });
        }

        let page_id = PageId(read_u64(bytes, PAGE_ID_OFFSET));
        if bytes[MAGIC_OFFSET..MAGIC_OFFSET + 4] != PAGE_MAGIC {
            return Err(StorageError::CorruptPage {
                page_id,
                reason: "invalid page magic".to_owned(),
            });
        }
        let version = read_u16(bytes, VERSION_OFFSET);
        if version != PAGE_FORMAT_VERSION {
            return Err(StorageError::CorruptPage {
                page_id,
                reason: format!("unsupported page format version {version}"),
            });
        }

        let expected_checksum = read_u32(bytes, CHECKSUM_OFFSET);
        let actual_checksum = checksum_with_zeroed_field(bytes);
        if expected_checksum != actual_checksum {
            return Err(StorageError::CorruptPage {
                page_id,
                reason: format!(
                    "checksum mismatch: stored {expected_checksum:#010x}, computed {actual_checksum:#010x}"
                ),
            });
        }

        let payload_length = read_u32(bytes, PAYLOAD_LENGTH_OFFSET) as usize;
        if payload_length > MAX_PAGE_PAYLOAD {
            return Err(StorageError::CorruptPage {
                page_id,
                reason: format!("payload length {payload_length} exceeds page capacity"),
            });
        }
        let payload = bytes[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + payload_length].to_vec();
        Ok(Self {
            id: page_id,
            lsn: Lsn(read_u64(bytes, LSN_OFFSET)),
            payload,
        })
    }
}

fn checksum_with_zeroed_field(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..CHECKSUM_OFFSET]);
    hasher.update(&[0_u8; 4]);
    hasher.update(&bytes[CHECKSUM_OFFSET + 4..]);
    hasher.finalize()
}

pub(crate) fn write_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

pub(crate) fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> u64 {
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
    fn page_round_trip_preserves_identity_lsn_and_payload() {
        let page = Page::with_lsn(PageId(19), Lsn(7), b"hello".to_vec()).expect("page");
        let decoded = Page::decode(&page.encode()).expect("decode page");
        assert_eq!(decoded, page);
    }

    #[test]
    fn checksum_detects_single_bit_corruption() {
        let page = Page::new(PageId(3), b"valuable data".to_vec()).expect("page");
        let mut bytes = page.encode();
        bytes[PAGE_HEADER_SIZE + 2] ^= 0b0000_0001;
        assert!(matches!(
            Page::decode(&bytes),
            Err(StorageError::CorruptPage { .. })
        ));
    }

    #[test]
    fn rejects_oversized_payloads() {
        let error = Page::new(PageId(1), vec![0; MAX_PAGE_PAYLOAD + 1])
            .expect_err("oversized page must fail");
        assert!(matches!(error, StorageError::PageTooLarge { .. }));
    }
}
