//! Persistent ordered indexes for QuantaDB.
//!
//! Index generations are immutable B+ trees. Builders write every node in one
//! atomic storage batch and callers publish the returned root only after that
//! batch is durable. Immutable generations make concurrent reads latch-free.

mod error;
mod node;
mod tree;

pub use error::{IndexError, Result};
pub use tree::{BPlusTree, IndexBuildPlan, IndexEntry, IndexMutation, IndexRoot};
