use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("invalid server configuration: {0}")]
    Configuration(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("request service task failed: {0}")]
    ServiceTask(String),

    #[error("database engine error: {0}")]
    Engine(#[from] quantadb_engine::EngineError),
}

pub type Result<T> = std::result::Result<T, ServerError>;
