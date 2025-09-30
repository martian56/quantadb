use thiserror::Error;

#[derive(Error, Debug)]
pub enum QuantaClientError {
    #[error("Connection error: {0}")]
    ConnectionError(String),
    
    #[error("Query execution error: {0}")]
    QueryError(String),
    
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, QuantaClientError>;
