//! QuantaDB network server.
//!
//! Protocol framing and connection management live here. Query execution and
//! storage will be attached behind the request dispatcher in later milestones.

mod config;
mod error;
pub mod pg;
pub mod protocol;
mod server;
mod service;

pub use config::ServerConfig;
pub use error::{Result, ServerError};
pub use server::QuantaServer;
pub use service::{EngineService, RequestService, RequestSession, SyntaxService};
