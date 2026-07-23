use crate::PageId;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("page payload is {actual} bytes; maximum is {maximum}")]
    PageTooLarge { actual: usize, maximum: usize },

    #[error("invalid page image length {actual}; expected {expected}")]
    InvalidPageLength { actual: usize, expected: usize },

    #[error("corrupt page {page_id}: {reason}")]
    CorruptPage { page_id: PageId, reason: String },

    #[error("corrupt data file: {0}")]
    CorruptDataFile(String),

    #[error("corrupt WAL at byte {offset}: {reason}")]
    CorruptWal { offset: u64, reason: String },

    #[error("page {0} does not exist")]
    PageNotFound(PageId),

    #[error("all {capacity} buffer frames are pinned")]
    BufferPoolExhausted { capacity: usize },

    #[error("storage directory is already open by another process: {0}")]
    AlreadyOpen(PathBuf),

    #[error("invalid storage configuration: {0}")]
    Configuration(String),

    #[error("group commit coordinator stopped")]
    CommitCoordinatorStopped,

    #[error("group commit failed: {0}")]
    GroupCommit(String),

    #[error("storage writer is poisoned after a failed durability operation; reopen to recover")]
    Poisoned,
}

pub type Result<T> = std::result::Result<T, StorageError>;
