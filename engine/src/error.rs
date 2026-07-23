use quantadb_mvcc::TransactionError;
use quantadb_syntax::{Span, SyntaxError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("transaction error: {0}")]
    Transaction(#[from] TransactionError),

    #[error("catalog encoding error: {0}")]
    Encoding(#[from] serde_json::Error),

    #[error("SQL syntax error: {message}")]
    Syntax { message: String, span: Span },

    #[error("corrupt engine record: {0}")]
    CorruptRecord(String),

    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("table does not exist: {0}")]
    TableNotFound(String),

    #[error("invalid schema: {0}")]
    InvalidSchema(String),

    #[error("invalid row: {0}")]
    InvalidRow(String),

    #[error("column does not exist: {0}")]
    ColumnNotFound(String),

    #[error("constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("expression error: {0}")]
    Expression(String),

    #[error("unsupported SQL feature: {0}")]
    Unsupported(String),

    #[error("a transaction is already active")]
    TransactionAlreadyActive,

    #[error("there is no active transaction")]
    NoActiveTransaction,

    #[error("the current transaction is aborted; ROLLBACK is required")]
    TransactionAborted,
}

impl From<SyntaxError> for EngineError {
    fn from(error: SyntaxError) -> Self {
        Self::Syntax {
            message: error.message().to_owned(),
            span: error.span(),
        }
    }
}

pub type Result<T> = std::result::Result<T, EngineError>;
