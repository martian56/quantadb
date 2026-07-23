use quantadb_index::IndexError;
use quantadb_storage::{PageId, StorageError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransactionError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("index error: {0}")]
    Index(#[from] IndexError),

    #[error("write conflict on key {key:?}")]
    WriteConflict { key: Vec<u8> },

    #[error("range conflict under scanned prefix {prefix:?}")]
    RangeConflict { prefix: Vec<u8> },

    #[error("transaction is no longer active")]
    Inactive,

    #[error("key/value record is {actual} bytes; maximum is {maximum}")]
    RecordTooLarge { actual: usize, maximum: usize },

    #[error("corrupt MVCC record on page {page_id}: {reason}")]
    CorruptRecord { page_id: PageId, reason: String },

    #[error("transaction ID space exhausted")]
    TransactionIdExhausted,

    #[error("commit timestamp space exhausted")]
    TimestampExhausted,

    #[error("MVCC state lock is poisoned")]
    StatePoisoned,
}

pub type Result<T> = std::result::Result<T, TransactionError>;
