use quantadb_storage::{PageId, StorageError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("index keys must be supplied in strictly increasing order")]
    KeysNotStrictlyIncreasing,

    #[error("index key is {actual} bytes; maximum for this node is {maximum}")]
    KeyTooLarge { actual: usize, maximum: usize },

    #[error("index entry count exceeds the supported range")]
    EntryCountOverflow,

    #[error("corrupt index node on page {page_id}: {reason}")]
    CorruptNode { page_id: PageId, reason: String },
}

pub type Result<T> = std::result::Result<T, IndexError>;
