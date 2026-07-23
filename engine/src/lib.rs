//! QuantaDB's relational catalog and SQL execution layer.
//!
//! The engine maps versioned schemas and rows onto MVCC byte keys. Networking
//! and protocol representation remain in the server crate.

mod access;
mod codec;
mod database;
mod error;
mod expression;
mod result;
mod schema;
mod value;

pub use database::{DatabaseEngine, SessionStatus, SqlSession};
pub use error::{EngineError, Result};
pub use result::{OutputColumn, StatementOutput, TransactionOutput};
pub use schema::{ColumnSchema, LogicalType, TableSchema};
pub use value::Value;
