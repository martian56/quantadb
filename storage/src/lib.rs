//! Durable storage primitives for QuantaDB.
//!
//! The crate owns stable on-disk formats, write-ahead logging, restart
//! recovery, and bounded page caching. SQL and transaction semantics do not
//! belong here.

mod buffer_pool;
mod data_file;
mod error;
mod group_commit;
mod page;
mod store;
mod wal;

pub use buffer_pool::{BufferPool, BufferPoolStats};
pub use error::{Result, StorageError};
pub use group_commit::{GroupCommitHandle, GroupCommitOptions, GroupCommitStats, GroupCommitter};
pub use page::{Lsn, Page, PageId, MAX_PAGE_PAYLOAD, PAGE_SIZE};
pub use store::{DurableStore, PageWrite, StoreOptions};
