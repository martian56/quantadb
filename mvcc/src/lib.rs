//! Durable multi-version concurrency control for QuantaDB.
//!
//! Transactions provide snapshot isolation with first-committer-wins write
//! conflicts. Immutable versions are persisted through atomic storage batches
//! before becoming visible in memory.

mod database;
mod error;
mod index_manifest;
mod record;

pub use database::{
    CommitResult, IndexBuildResult, MvccDatabase, MvccOptions, MvccStats, Timestamp, Transaction,
    TransactionId,
};
pub use error::{Result, TransactionError};
