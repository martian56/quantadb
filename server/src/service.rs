use crate::protocol::{ColumnMetadata, ScalarValue, StatementResult, TransactionState};
use crate::protocol::{ErrorCode, ProtocolError, Request, Response, SERVER_VERSION};
use quantadb_engine::{
    DatabaseEngine, EngineError, LogicalType, SqlSession, StatementOutput, TransactionOutput, Value,
};
use quantadb_syntax::parse_sql;

/// The database-facing boundary owned by the network server.
///
/// `open_session` is called once per accepted connection and must be cheap.
/// Session request handling runs on Tokio's blocking pool.
pub trait RequestService: Send + Sync + 'static {
    fn open_session(&self) -> Box<dyn RequestSession>;
}

/// Stateful request handling for one client connection.
pub trait RequestSession: Send + 'static {
    fn handle(&mut self, request: Request) -> Response;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SyntaxService;

impl RequestService for SyntaxService {
    fn open_session(&self) -> Box<dyn RequestSession> {
        Box::new(SyntaxSession)
    }
}

#[derive(Clone)]
pub struct EngineService {
    engine: DatabaseEngine,
}

impl EngineService {
    #[must_use]
    pub const fn new(engine: DatabaseEngine) -> Self {
        Self { engine }
    }
}

impl RequestService for EngineService {
    fn open_session(&self) -> Box<dyn RequestSession> {
        Box::new(EngineSession {
            sql: self.engine.session(),
        })
    }
}

struct EngineSession {
    sql: SqlSession,
}

impl RequestSession for EngineSession {
    fn handle(&mut self, request: Request) -> Response {
        match request {
            Request::Ping => Response::Pong {
                server_version: SERVER_VERSION.to_owned(),
            },
            Request::Parse { sql } => match parse_sql(&sql) {
                Ok(statements) => Response::Parsed { statements },
                Err(error) => syntax_error(error.message(), error.span()),
            },
            Request::Execute { sql } => match self.sql.execute(&sql) {
                Ok(results) => Response::Executed {
                    results: results.into_iter().map(map_output).collect(),
                },
                Err(error) => map_engine_error(error),
            },
        }
    }
}

fn map_output(output: StatementOutput) -> StatementResult {
    match output {
        StatementOutput::Transaction(state) => StatementResult::Transaction {
            state: match state {
                TransactionOutput::Begun => TransactionState::Begun,
                TransactionOutput::Committed => TransactionState::Committed,
                TransactionOutput::RolledBack => TransactionState::RolledBack,
            },
        },
        StatementOutput::Command { tag, affected_rows } => {
            StatementResult::Command { tag, affected_rows }
        }
        StatementOutput::Query { columns, rows } => StatementResult::Query {
            columns: columns
                .into_iter()
                .map(|column| ColumnMetadata {
                    name: column.name,
                    data_type: logical_type_name(&column.data_type),
                    nullable: column.nullable,
                })
                .collect(),
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(map_value).collect())
                .collect(),
        },
    }
}

fn map_value(value: Value) -> ScalarValue {
    match value {
        Value::Null => ScalarValue::Null,
        Value::Boolean(value) => ScalarValue::Boolean(value),
        Value::Integer(value) => ScalarValue::Integer(value),
        Value::Float(value) => ScalarValue::Float(value),
        Value::Text(value) => ScalarValue::Text(value),
    }
}

fn logical_type_name(data_type: &LogicalType) -> String {
    match data_type {
        LogicalType::Unknown => "unknown".to_owned(),
        LogicalType::Boolean => "boolean".to_owned(),
        LogicalType::Int64 => "int64".to_owned(),
        LogicalType::Float64 => "float64".to_owned(),
        LogicalType::Text { max_length: None } => "text".to_owned(),
        LogicalType::Text {
            max_length: Some(maximum),
        } => format!("varchar({maximum})"),
    }
}

fn map_engine_error(error: EngineError) -> Response {
    match error {
        EngineError::Syntax { message, span } => syntax_error(&message, span),
        error @ (EngineError::TransactionAlreadyActive
        | EngineError::NoActiveTransaction
        | EngineError::TransactionAborted
        | EngineError::Transaction(_)) => Response::Error {
            error: ProtocolError {
                code: ErrorCode::TransactionError,
                message: error.to_string(),
                span: None,
            },
        },
        error => Response::Error {
            error: ProtocolError {
                code: ErrorCode::ExecutionError,
                message: error.to_string(),
                span: None,
            },
        },
    }
}

fn syntax_error(message: &str, span: quantadb_syntax::Span) -> Response {
    Response::Error {
        error: ProtocolError {
            code: ErrorCode::SyntaxError,
            message: message.to_owned(),
            span: Some(span),
        },
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SyntaxSession;

impl RequestSession for SyntaxSession {
    fn handle(&mut self, request: Request) -> Response {
        match request {
            Request::Ping => Response::Pong {
                server_version: SERVER_VERSION.to_owned(),
            },
            Request::Parse { sql } => match parse_sql(&sql) {
                Ok(statements) => Response::Parsed { statements },
                Err(error) => Response::Error {
                    error: ProtocolError {
                        code: crate::protocol::ErrorCode::SyntaxError,
                        message: error.message().to_owned(),
                        span: Some(error.span()),
                    },
                },
            },
            Request::Execute { sql } => match parse_sql(&sql) {
                Err(error) => Response::Error {
                    error: ProtocolError {
                        code: ErrorCode::SyntaxError,
                        message: error.message().to_owned(),
                        span: Some(error.span()),
                    },
                },
                Ok(_) => Response::Error {
                    error: ProtocolError {
                        code: ErrorCode::ExecutionUnavailable,
                        message: "this server has no SQL execution service configured".to_owned(),
                        span: None,
                    },
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syntax_service_preserves_ping_parse_and_diagnostics() {
        let service = SyntaxService;
        assert!(matches!(
            service.open_session().handle(Request::Ping),
            Response::Pong { .. }
        ));
        assert!(matches!(
            service.open_session().handle(Request::Parse {
                sql: "BEGIN; CREATE INDEX idx ON records (id)".to_owned(),
            }),
            Response::Parsed { statements } if statements.len() == 2
        ));
        assert!(matches!(
            service.open_session().handle(Request::Parse {
                sql: "SELECT FROM".to_owned(),
            }),
            Response::Error {
                error: ProtocolError { span: Some(_), .. }
            }
        ));
        assert!(matches!(
            service.open_session().handle(Request::Execute {
                sql: "SELECT id FROM records".to_owned(),
            }),
            Response::Error {
                error: ProtocolError {
                    code: ErrorCode::ExecutionUnavailable,
                    ..
                }
            }
        ));
    }
}
