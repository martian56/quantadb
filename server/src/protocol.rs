use quantadb_syntax::{Span, Statement};
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestFrame {
    pub protocol_version: u16,
    pub request_id: u64,
    pub request: Request,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Ping,
    Parse { sql: String },
    Execute { sql: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseFrame {
    pub protocol_version: u16,
    pub request_id: u64,
    pub response: Response,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Pong { server_version: String },
    Parsed { statements: Vec<Statement> },
    Executed { results: Vec<StatementResult> },
    Error { error: ProtocolError },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StatementResult {
    Transaction {
        state: TransactionState,
    },
    Command {
        tag: String,
        affected_rows: u64,
    },
    Query {
        columns: Vec<ColumnMetadata>,
        rows: Vec<Vec<ScalarValue>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionState {
    Begun,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Text(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidJson,
    UnsupportedProtocolVersion,
    SyntaxError,
    ExecutionUnavailable,
    ExecutionError,
    TransactionError,
    FrameTooLarge,
    IdleTimeout,
    ServerBusy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<Span>,
}

impl ResponseFrame {
    #[must_use]
    pub fn pong(request_id: u64) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            response: Response::Pong {
                server_version: SERVER_VERSION.to_owned(),
            },
        }
    }

    #[must_use]
    pub fn parsed(request_id: u64, statements: Vec<Statement>) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            response: Response::Parsed { statements },
        }
    }

    #[must_use]
    pub fn error(
        request_id: u64,
        code: ErrorCode,
        message: impl Into<String>,
        span: Option<Span>,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            response: Response::Error {
                error: ProtocolError {
                    code,
                    message: message.into(),
                    span,
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_is_explicitly_tagged_and_versioned() {
        let frame = RequestFrame {
            protocol_version: PROTOCOL_VERSION,
            request_id: 42,
            request: Request::Ping,
        };
        let json = serde_json::to_string(&frame).expect("request should serialize");
        assert!(json.contains("\"protocol_version\":1"));
        assert!(json.contains("\"type\":\"ping\""));

        let decoded: RequestFrame =
            serde_json::from_str(&json).expect("request should deserialize");
        assert_eq!(decoded, frame);
    }
}
