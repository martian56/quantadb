use thiserror::Error;

#[derive(Error, Debug)]
pub enum QuantaError {
    #[error("SQL parsing error: {0}")]
    SqlParseError(String),
    
    #[error("Storage error: {0}")]
    StorageError(String),
    
    #[error("Network error: {0}")]
    NetworkError(String),
    
    #[error("Query execution error: {0}")]
    QueryError(String),
    
    #[error("Table '{0}' not found")]
    TableNotFound(String),
    
    #[error("Column '{0}' not found")]
    ColumnNotFound(String),
    
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, QuantaError>;
