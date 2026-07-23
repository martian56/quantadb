use crate::{IndexEntry, IndexError, Result};
use quantadb_storage::{PageId, MAX_PAGE_PAYLOAD};

const MAGIC: [u8; 4] = *b"QNIX";
const FORMAT_VERSION: u16 = 1;
const HEADER_SIZE: usize = 32;
const LEAF_KIND: u8 = 1;
const INTERNAL_KIND: u8 = 2;
const NO_PAGE: u64 = u64::MAX;
const LEAF_ENTRY_OVERHEAD: usize = 12;
const INTERNAL_ENTRY_OVERHEAD: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Node {
    Leaf {
        entries: Vec<IndexEntry>,
        next: Option<PageId>,
    },
    Internal {
        level: u16,
        first_child: PageId,
        separators: Vec<(Vec<u8>, PageId)>,
    },
}

impl Node {
    pub(crate) fn level(&self) -> u16 {
        match self {
            Self::Leaf { .. } => 0,
            Self::Internal { level, .. } => *level,
        }
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let encoded_length = match self {
            Self::Leaf { entries, .. } => {
                entries.iter().try_fold(HEADER_SIZE, |length, entry| {
                    checked_entry_length(length, entry.key.len(), LEAF_ENTRY_OVERHEAD)
                })?
            }
            Self::Internal { separators, .. } => separators
                .iter()
                .try_fold(HEADER_SIZE, |length, (key, _)| {
                    checked_entry_length(length, key.len(), INTERNAL_ENTRY_OVERHEAD)
                })?,
        };
        if encoded_length > MAX_PAGE_PAYLOAD {
            return Err(IndexError::KeyTooLarge {
                actual: encoded_length,
                maximum: MAX_PAGE_PAYLOAD,
            });
        }

        let mut bytes = vec![0_u8; encoded_length];
        bytes[0..4].copy_from_slice(&MAGIC);
        write_u16(&mut bytes, 4, FORMAT_VERSION);
        write_u16(&mut bytes, 12, self.level());

        match self {
            Self::Leaf { entries, next } => {
                bytes[6] = LEAF_KIND;
                write_u32(&mut bytes, 8, count_u32(entries.len())?);
                write_u64(&mut bytes, 16, next.map_or(NO_PAGE, |page_id| page_id.0));
                let mut cursor = HEADER_SIZE;
                for entry in entries {
                    write_key(&mut bytes, &mut cursor, &entry.key);
                    write_u64(&mut bytes, cursor, entry.value.0);
                    cursor += 8;
                }
            }
            Self::Internal {
                first_child,
                separators,
                ..
            } => {
                bytes[6] = INTERNAL_KIND;
                write_u32(&mut bytes, 8, count_u32(separators.len())?);
                write_u64(&mut bytes, 16, first_child.0);
                let mut cursor = HEADER_SIZE;
                for (key, child) in separators {
                    write_key(&mut bytes, &mut cursor, key);
                    write_u64(&mut bytes, cursor, child.0);
                    cursor += 8;
                }
            }
        }
        Ok(bytes)
    }

    pub(crate) fn decode(page_id: PageId, bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(corrupt(page_id, "node is shorter than its header"));
        }
        if bytes[0..4] != MAGIC {
            return Err(corrupt(page_id, "invalid node magic"));
        }
        let version = read_u16(bytes, 4);
        if version != FORMAT_VERSION {
            return Err(corrupt(
                page_id,
                format!("unsupported node format version {version}"),
            ));
        }
        if bytes[7] != 0
            || bytes[14..16].iter().any(|byte| *byte != 0)
            || bytes[24..32].iter().any(|byte| *byte != 0)
        {
            return Err(corrupt(page_id, "reserved header bytes are not zero"));
        }

        let kind = bytes[6];
        let count = read_u32(bytes, 8) as usize;
        let level = read_u16(bytes, 12);
        let auxiliary = read_u64(bytes, 16);
        let mut cursor = HEADER_SIZE;

        match kind {
            LEAF_KIND => {
                if level != 0 {
                    return Err(corrupt(page_id, "leaf level must be zero"));
                }
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = read_key(page_id, bytes, &mut cursor)?;
                    let value = PageId(read_required_u64(page_id, bytes, &mut cursor)?);
                    entries.push(IndexEntry { key, value });
                }
                ensure_finished(page_id, bytes, cursor)?;
                ensure_strict_keys(page_id, entries.iter().map(|entry| entry.key.as_slice()))?;
                Ok(Self::Leaf {
                    entries,
                    next: (auxiliary != NO_PAGE).then_some(PageId(auxiliary)),
                })
            }
            INTERNAL_KIND => {
                if level == 0 {
                    return Err(corrupt(page_id, "internal level must be positive"));
                }
                if auxiliary == NO_PAGE {
                    return Err(corrupt(page_id, "internal node has no first child"));
                }
                let mut separators = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = read_key(page_id, bytes, &mut cursor)?;
                    let child = PageId(read_required_u64(page_id, bytes, &mut cursor)?);
                    if child.0 == NO_PAGE {
                        return Err(corrupt(page_id, "separator has no child"));
                    }
                    separators.push((key, child));
                }
                ensure_finished(page_id, bytes, cursor)?;
                ensure_strict_keys(page_id, separators.iter().map(|(key, _)| key.as_slice()))?;
                Ok(Self::Internal {
                    level,
                    first_child: PageId(auxiliary),
                    separators,
                })
            }
            _ => Err(corrupt(page_id, format!("unknown node kind {kind}"))),
        }
    }
}

pub(crate) const fn leaf_entry_size(key_length: usize) -> Option<usize> {
    key_length.checked_add(LEAF_ENTRY_OVERHEAD)
}

pub(crate) const fn internal_entry_size(key_length: usize) -> Option<usize> {
    key_length.checked_add(INTERNAL_ENTRY_OVERHEAD)
}

pub(crate) const fn header_size() -> usize {
    HEADER_SIZE
}

fn checked_entry_length(length: usize, key_length: usize, overhead: usize) -> Result<usize> {
    length
        .checked_add(key_length)
        .and_then(|length| length.checked_add(overhead))
        .ok_or(IndexError::KeyTooLarge {
            actual: usize::MAX,
            maximum: MAX_PAGE_PAYLOAD,
        })
}

fn count_u32(count: usize) -> Result<u32> {
    u32::try_from(count).map_err(|_| IndexError::EntryCountOverflow)
}

fn write_key(bytes: &mut [u8], cursor: &mut usize, key: &[u8]) {
    write_u32(bytes, *cursor, key.len() as u32);
    *cursor += 4;
    bytes[*cursor..*cursor + key.len()].copy_from_slice(key);
    *cursor += key.len();
}

fn read_key(page_id: PageId, bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let key_length = read_required_u32(page_id, bytes, cursor)? as usize;
    let end = cursor
        .checked_add(key_length)
        .ok_or_else(|| corrupt(page_id, "key length overflow"))?;
    if end > bytes.len() {
        return Err(corrupt(page_id, "key extends past node boundary"));
    }
    let key = bytes[*cursor..end].to_vec();
    *cursor = end;
    Ok(key)
}

fn read_required_u32(page_id: PageId, bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| corrupt(page_id, "field offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt(page_id, "node ends inside a 32-bit field"));
    }
    let value = read_u32(bytes, *cursor);
    *cursor = end;
    Ok(value)
}

fn read_required_u64(page_id: PageId, bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| corrupt(page_id, "field offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt(page_id, "node ends inside a 64-bit field"));
    }
    let value = read_u64(bytes, *cursor);
    *cursor = end;
    Ok(value)
}

fn ensure_finished(page_id: PageId, bytes: &[u8], cursor: usize) -> Result<()> {
    if cursor != bytes.len() {
        return Err(corrupt(
            page_id,
            format!(
                "node contains {} trailing bytes",
                bytes.len().saturating_sub(cursor)
            ),
        ));
    }
    Ok(())
}

fn ensure_strict_keys<'a>(page_id: PageId, mut keys: impl Iterator<Item = &'a [u8]>) -> Result<()> {
    let Some(mut previous) = keys.next() else {
        return Ok(());
    };
    for key in keys {
        if previous >= key {
            return Err(corrupt(page_id, "node keys are not strictly increasing"));
        }
        previous = key;
    }
    Ok(())
}

fn corrupt(page_id: PageId, reason: impl Into<String>) -> IndexError {
    IndexError::CorruptNode {
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
    fn leaf_and_internal_nodes_round_trip() {
        let nodes = [
            Node::Leaf {
                entries: vec![
                    IndexEntry {
                        key: b"a".to_vec(),
                        value: PageId(2),
                    },
                    IndexEntry {
                        key: b"b".to_vec(),
                        value: PageId(3),
                    },
                ],
                next: Some(PageId(8)),
            },
            Node::Internal {
                level: 2,
                first_child: PageId(4),
                separators: vec![(b"k".to_vec(), PageId(5))],
            },
        ];

        for node in nodes {
            assert_eq!(
                Node::decode(PageId(1), &node.encode().expect("encode")).expect("decode"),
                node
            );
        }
    }

    #[test]
    fn decoder_rejects_truncated_and_noncanonical_nodes() {
        let node = Node::Leaf {
            entries: vec![IndexEntry {
                key: b"a".to_vec(),
                value: PageId(2),
            }],
            next: None,
        };
        let encoded = node.encode().expect("encode");
        assert!(matches!(
            Node::decode(PageId(7), &encoded[..encoded.len() - 1]),
            Err(IndexError::CorruptNode { .. })
        ));

        let mut reserved = encoded;
        reserved[24] = 1;
        assert!(matches!(
            Node::decode(PageId(7), &reserved),
            Err(IndexError::CorruptNode { .. })
        ));
    }
}
