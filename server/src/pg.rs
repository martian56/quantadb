//! A PostgreSQL v3 wire protocol frontend.
//!
//! This speaks the simple query protocol: startup, SSL negotiation, trust
//! authentication, `Query`, transactions, and errors with SQLSTATEs. That is
//! enough for psql and for drivers that do not use prepared statements. The
//! extended query protocol is answered with a clear error until it is
//! implemented.
//!
//! Each accepted connection runs on a blocking thread with its own engine
//! session, bounded by the shared connection limit. The engine is
//! synchronous, so a thread per Postgres connection matches how work is
//! already scheduled elsewhere in the server.

use crate::Result;
use quantadb_engine::{
    DatabaseEngine, EngineError, LogicalType, SessionStatus, SqlSession, StatementOutput,
    TransactionOutput, Value,
};
use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::TcpStream,
    sync::Arc,
};
use tokio::{net::TcpListener, sync::Semaphore};
use tracing::{debug, warn};

const PROTOCOL_VERSION_3: i32 = 196_608;
const SSL_REQUEST_CODE: i32 = 80_877_103;
const GSSENC_REQUEST_CODE: i32 = 80_877_104;
const CANCEL_REQUEST_CODE: i32 = 80_877_102;
const MAX_STARTUP_BYTES: i32 = 65_536;
const MAX_QUERY_BYTES: i32 = 4 << 20;

const OID_BOOL: i32 = 16;
const OID_INT8: i32 = 20;
const OID_TEXT: i32 = 25;
const OID_FLOAT8: i32 = 701;

/// Accept Postgres connections until shutdown resolves.
pub async fn serve_postgres<F>(
    listener: TcpListener,
    engine: DatabaseEngine,
    max_connections: usize,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()>,
{
    let limiter = Arc::new(Semaphore::new(max_connections));
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        warn!(%error, "postgres accept failed");
                        continue;
                    }
                };
                let Ok(permit) = Arc::clone(&limiter).try_acquire_owned() else {
                    debug!(%peer, "postgres connection limit reached");
                    continue;
                };
                let session = engine.session();
                match socket.into_std() {
                    Ok(socket) => {
                        tokio::task::spawn_blocking(move || {
                            let _permit = permit;
                            if socket.set_nonblocking(false).is_err() {
                                return;
                            }
                            if let Err(error) = run_connection(socket, session) {
                                debug!(%peer, %error, "postgres connection ended");
                            }
                        });
                    }
                    Err(error) => warn!(%peer, %error, "postgres socket conversion failed"),
                }
            }
        }
    }
}

fn run_connection(socket: TcpStream, mut session: SqlSession) -> std::io::Result<()> {
    socket.set_nodelay(true).ok();
    let mut reader = BufReader::new(socket.try_clone()?);
    let mut writer = BufWriter::new(socket);

    if !handshake(&mut reader, &mut writer)? {
        return Ok(());
    }

    loop {
        let mut kind = [0_u8; 1];
        if reader.read_exact(&mut kind).is_err() {
            return Ok(());
        }
        let length = read_i32(&mut reader)?;
        if !(4..=MAX_QUERY_BYTES).contains(&length) {
            send_error(&mut writer, "08P01", "message length is out of range")?;
            return Ok(());
        }
        let mut payload = vec![0_u8; (length - 4) as usize];
        reader.read_exact(&mut payload)?;

        match kind[0] {
            b'Q' => {
                let sql = cstring_at(&payload, 0).unwrap_or_default();
                run_simple_query(&mut writer, &mut session, &sql)?;
            }
            b'X' => return Ok(()),
            b'S' => {
                send_ready(&mut writer, &session)?;
            }
            other => {
                send_error(
                    &mut writer,
                    "0A000",
                    &format!(
                        "message '{}' is not supported yet; use the simple query protocol",
                        char::from(other)
                    ),
                )?;
                send_ready(&mut writer, &session)?;
            }
        }
    }
}

/// Negotiate the startup phase. Returns false when the client went away.
fn handshake(reader: &mut impl Read, writer: &mut BufWriter<TcpStream>) -> std::io::Result<bool> {
    loop {
        let length = read_i32(reader)?;
        if !(8..=MAX_STARTUP_BYTES).contains(&length) {
            return Ok(false);
        }
        let code = read_i32(reader)?;
        let mut rest = vec![0_u8; (length - 8) as usize];
        reader.read_exact(&mut rest)?;

        match code {
            SSL_REQUEST_CODE | GSSENC_REQUEST_CODE => {
                writer.write_all(b"N")?;
                writer.flush()?;
            }
            CANCEL_REQUEST_CODE => return Ok(false),
            PROTOCOL_VERSION_3 => {
                write_message(writer, b'R', &0_i32.to_be_bytes())?;
                write_parameter_status(writer, "server_version", "16.0 (QuantaDB 0.2)")?;
                write_parameter_status(writer, "server_encoding", "UTF8")?;
                write_parameter_status(writer, "client_encoding", "UTF8")?;
                write_parameter_status(writer, "DateStyle", "ISO, MDY")?;
                let mut key_data = Vec::with_capacity(8);
                key_data.extend_from_slice(&1_i32.to_be_bytes());
                key_data.extend_from_slice(&1_i32.to_be_bytes());
                write_message(writer, b'K', &key_data)?;
                write_message(writer, b'Z', b"I")?;
                writer.flush()?;
                return Ok(true);
            }
            _ => {
                let mut body = Vec::new();
                error_fields(
                    &mut body,
                    "08P01",
                    "unsupported protocol version; QuantaDB speaks protocol 3.0",
                );
                write_message(writer, b'E', &body)?;
                writer.flush()?;
                return Ok(false);
            }
        }
    }
}

fn run_simple_query(
    writer: &mut BufWriter<TcpStream>,
    session: &mut SqlSession,
    sql: &str,
) -> std::io::Result<()> {
    if sql.trim().is_empty() {
        write_message(writer, b'I', &[])?;
        send_ready(writer, session)?;
        return Ok(());
    }

    match session.execute(sql) {
        Ok(outputs) => {
            for output in outputs {
                write_output(writer, output)?;
            }
        }
        Err(error) => {
            send_error(writer, sqlstate(&error), &error.to_string())?;
        }
    }
    send_ready(writer, session)
}

fn write_output(writer: &mut BufWriter<TcpStream>, output: StatementOutput) -> std::io::Result<()> {
    match output {
        StatementOutput::Transaction(state) => {
            let tag = match state {
                TransactionOutput::Begun => "BEGIN",
                TransactionOutput::Committed => "COMMIT",
                TransactionOutput::RolledBack => "ROLLBACK",
            };
            write_command_complete(writer, tag)
        }
        StatementOutput::Command { tag, affected_rows } => {
            let text = if tag == "INSERT" {
                format!("INSERT 0 {affected_rows}")
            } else {
                format!("{tag} {affected_rows}")
            };
            write_command_complete(writer, &text)
        }
        StatementOutput::Query { columns, rows } => {
            let mut body = Vec::new();
            body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
            for column in &columns {
                body.extend_from_slice(column.name.as_bytes());
                body.push(0);
                body.extend_from_slice(&0_i32.to_be_bytes());
                body.extend_from_slice(&0_i16.to_be_bytes());
                body.extend_from_slice(&type_oid(&column.data_type).to_be_bytes());
                body.extend_from_slice(&(-1_i16).to_be_bytes());
                body.extend_from_slice(&(-1_i32).to_be_bytes());
                body.extend_from_slice(&0_i16.to_be_bytes());
            }
            write_message(writer, b'T', &body)?;

            let row_count = rows.len();
            for row in rows {
                let mut body = Vec::new();
                body.extend_from_slice(&(row.len() as i16).to_be_bytes());
                for value in row {
                    match value_text(&value) {
                        Some(text) => {
                            body.extend_from_slice(&(text.len() as i32).to_be_bytes());
                            body.extend_from_slice(text.as_bytes());
                        }
                        None => body.extend_from_slice(&(-1_i32).to_be_bytes()),
                    }
                }
                write_message(writer, b'D', &body)?;
            }
            write_command_complete(writer, &format!("SELECT {row_count}"))
        }
    }
}

fn send_ready(writer: &mut BufWriter<TcpStream>, session: &SqlSession) -> std::io::Result<()> {
    let status = match session.transaction_status() {
        SessionStatus::Idle => b"I",
        SessionStatus::InTransaction => b"T",
        SessionStatus::Failed => b"E",
    };
    write_message(writer, b'Z', status)?;
    writer.flush()
}

fn send_error(writer: &mut BufWriter<TcpStream>, code: &str, message: &str) -> std::io::Result<()> {
    let mut body = Vec::new();
    error_fields(&mut body, code, message);
    write_message(writer, b'E', &body)?;
    writer.flush()
}

fn error_fields(body: &mut Vec<u8>, code: &str, message: &str) {
    for (field, text) in [
        (b'S', "ERROR"),
        (b'V', "ERROR"),
        (b'C', code),
        (b'M', message),
    ] {
        body.push(field);
        body.extend_from_slice(text.as_bytes());
        body.push(0);
    }
    body.push(0);
}

fn sqlstate(error: &EngineError) -> &'static str {
    match error {
        EngineError::Syntax { .. } => "42601",
        EngineError::TableNotFound(_) => "42P01",
        EngineError::TableAlreadyExists(_) | EngineError::IndexAlreadyExists(_) => "42P07",
        EngineError::IndexNotFound(_) => "42704",
        EngineError::ColumnNotFound(_) => "42703",
        EngineError::ConstraintViolation(_) => "23505",
        EngineError::Transaction(_) => "40001",
        EngineError::TransactionAborted => "25P02",
        EngineError::TransactionAlreadyActive | EngineError::NoActiveTransaction => "25000",
        EngineError::Unsupported(_) => "0A000",
        _ => "XX000",
    }
}

fn type_oid(data_type: &LogicalType) -> i32 {
    match data_type {
        LogicalType::Boolean => OID_BOOL,
        LogicalType::Int64 => OID_INT8,
        LogicalType::Float64 => OID_FLOAT8,
        LogicalType::Text { .. } | LogicalType::Unknown => OID_TEXT,
    }
}

fn value_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Boolean(true) => Some("t".to_owned()),
        Value::Boolean(false) => Some("f".to_owned()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Float(value) => Some(value.to_string()),
        Value::Text(value) => Some(value.clone()),
    }
}

fn write_command_complete(writer: &mut BufWriter<TcpStream>, tag: &str) -> std::io::Result<()> {
    let mut body = tag.as_bytes().to_vec();
    body.push(0);
    write_message(writer, b'C', &body)
}

fn write_parameter_status(
    writer: &mut BufWriter<TcpStream>,
    name: &str,
    value: &str,
) -> std::io::Result<()> {
    let mut body = Vec::with_capacity(name.len() + value.len() + 2);
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    write_message(writer, b'S', &body)
}

fn write_message(writer: &mut BufWriter<TcpStream>, kind: u8, body: &[u8]) -> std::io::Result<()> {
    writer.write_all(&[kind])?;
    writer.write_all(&((body.len() as i32) + 4).to_be_bytes())?;
    writer.write_all(body)
}

fn read_i32(reader: &mut impl Read) -> std::io::Result<i32> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(i32::from_be_bytes(bytes))
}

fn cstring_at(payload: &[u8], start: usize) -> Option<String> {
    let end = payload[start..].iter().position(|byte| *byte == 0)? + start;
    String::from_utf8(payload[start..end].to_vec()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use quantadb_mvcc::MvccOptions;
    use tempfile::tempdir;

    struct PgClient {
        stream: TcpStream,
    }

    impl PgClient {
        fn connect(address: std::net::SocketAddr) -> Self {
            let mut stream = TcpStream::connect(address).expect("connect");
            let mut startup = Vec::new();
            startup.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
            startup.extend_from_slice(b"user\0quanta\0database\0quanta\0\0");
            let mut framed = ((startup.len() as i32) + 4).to_be_bytes().to_vec();
            framed.extend_from_slice(&startup);
            stream.write_all(&framed).expect("startup");
            Self { stream }
        }

        fn query(&mut self, sql: &str) {
            let mut body = sql.as_bytes().to_vec();
            body.push(0);
            let mut framed = vec![b'Q'];
            framed.extend_from_slice(&((body.len() as i32) + 4).to_be_bytes());
            framed.extend_from_slice(&body);
            self.stream.write_all(&framed).expect("query");
        }

        /// Read messages until ReadyForQuery, returning (kind, body) pairs.
        fn read_until_ready(&mut self) -> Vec<(u8, Vec<u8>)> {
            let mut messages = Vec::new();
            loop {
                let mut kind = [0_u8; 1];
                self.stream.read_exact(&mut kind).expect("message kind");
                let mut length = [0_u8; 4];
                self.stream.read_exact(&mut length).expect("length");
                let length = i32::from_be_bytes(length) - 4;
                let mut body = vec![0_u8; length as usize];
                self.stream.read_exact(&mut body).expect("body");
                let done = kind[0] == b'Z';
                messages.push((kind[0], body));
                if done {
                    return messages;
                }
            }
        }
    }

    fn kinds(messages: &[(u8, Vec<u8>)]) -> Vec<u8> {
        messages.iter().map(|(kind, _)| *kind).collect()
    }

    #[tokio::test]
    async fn speaks_the_simple_query_protocol_end_to_end() {
        let directory = tempdir().expect("tempdir");
        let engine =
            DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        tokio::spawn(serve_postgres(listener, engine, 8, std::future::pending()));

        let exchange = tokio::task::spawn_blocking(move || {
            let mut client = PgClient::connect(address);
            let startup = client.read_until_ready();
            assert_eq!(startup.first().map(|(kind, _)| *kind), Some(b'R'));
            assert_eq!(startup.last().map(|(kind, _)| *kind), Some(b'Z'));

            client
                .query("CREATE TABLE pets (id BIGINT PRIMARY KEY, name TEXT NOT NULL, good BOOL)");
            let created = client.read_until_ready();
            assert_eq!(kinds(&created), vec![b'C', b'Z']);

            client.query(
                "INSERT INTO pets (id, name, good) VALUES (1, 'rex', true), (2, 'ada', NULL)",
            );
            let inserted = client.read_until_ready();
            assert!(String::from_utf8_lossy(&inserted[0].1).starts_with("INSERT 0 2"));

            client.query("SELECT id, name, good FROM pets ORDER BY id");
            let selected = client.read_until_ready();
            assert_eq!(kinds(&selected), vec![b'T', b'D', b'D', b'C', b'Z']);
            let first_row = &selected[1].1;
            assert_eq!(i16::from_be_bytes([first_row[0], first_row[1]]), 3);
            let text = String::from_utf8_lossy(first_row);
            assert!(text.contains("rex"), "{text:?}");
            let second_row = &selected[2].1;
            assert!(
                second_row.ends_with(&(-1_i32).to_be_bytes()),
                "a NULL column arrives as length -1"
            );

            client.query("SELECT nope FROM pets");
            let failed = client.read_until_ready();
            assert_eq!(kinds(&failed), vec![b'E', b'Z']);
            let error_text = String::from_utf8_lossy(&failed[0].1);
            assert!(error_text.contains("42703"), "{error_text:?}");

            client.query("BEGIN");
            let begun = client.read_until_ready();
            assert_eq!(
                begun.last().map(|(_, body)| body.clone()),
                Some(b"T".to_vec()),
                "ReadyForQuery reports an open transaction"
            );
            client.query("ROLLBACK");
            client.read_until_ready();
        });
        exchange.await.expect("client thread");
    }

    #[tokio::test]
    async fn refuses_ssl_politely_and_keeps_talking() {
        let directory = tempdir().expect("tempdir");
        let engine =
            DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        tokio::spawn(serve_postgres(listener, engine, 8, std::future::pending()));

        let exchange = tokio::task::spawn_blocking(move || {
            let mut stream = TcpStream::connect(address).expect("connect");
            let mut ssl_request = 8_i32.to_be_bytes().to_vec();
            ssl_request.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
            stream.write_all(&ssl_request).expect("ssl request");
            let mut answer = [0_u8; 1];
            stream.read_exact(&mut answer).expect("ssl answer");
            assert_eq!(answer[0], b'N', "no TLS yet, and no hang either");

            let mut client = PgClient { stream };
            let mut startup = Vec::new();
            startup.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
            startup.extend_from_slice(b"user\0quanta\0\0");
            let mut framed = ((startup.len() as i32) + 4).to_be_bytes().to_vec();
            framed.extend_from_slice(&startup);
            client.stream.write_all(&framed).expect("startup");
            let ready = client.read_until_ready();
            assert_eq!(ready.last().map(|(kind, _)| *kind), Some(b'Z'));
        });
        exchange.await.expect("client thread");
    }
}
