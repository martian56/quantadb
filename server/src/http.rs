//! A minimal HTTP bridge so browsers can reach QuantaDB.
//!
//! One endpoint: `POST /v1/execute` with a JSON body `{"sql": "..."}`,
//! answered with `{"results": [...]}` in the same shapes protocol v1 uses,
//! or `{"error": {...}}` with a protocol error code. CORS headers let a
//! static page call it, and OPTIONS preflights get a friendly 204.
//!
//! Requests are stateless: each one runs in its own engine session, so a
//! transaction lives inside a single request body, `BEGIN` through
//! `COMMIT` in one execute call. The bridge is off by default until
//! authentication exists.

use crate::{
    protocol::{Response as ProtocolResponse, StatementResult},
    service::{map_engine_error, map_output},
    Result,
};
use quantadb_engine::DatabaseEngine;
use serde::Deserialize;
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    sync::Arc,
};
use tokio::{net::TcpListener, sync::Semaphore};
use tracing::{debug, warn};

const MAX_BODY_BYTES: usize = 4 << 20;
const MAX_HEADER_LINES: usize = 100;

#[derive(Deserialize)]
struct ExecuteBody {
    sql: String,
}

/// Accept HTTP connections until shutdown resolves.
pub async fn serve_http<F>(
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
                        warn!(%error, "http accept failed");
                        continue;
                    }
                };
                let Ok(permit) = Arc::clone(&limiter).try_acquire_owned() else {
                    debug!(%peer, "http connection limit reached");
                    continue;
                };
                let engine = engine.clone();
                match socket.into_std() {
                    Ok(socket) => {
                        tokio::task::spawn_blocking(move || {
                            let _permit = permit;
                            if socket.set_nonblocking(false).is_err() {
                                return;
                            }
                            if let Err(error) = run_connection(socket, &engine) {
                                debug!(%peer, %error, "http connection ended");
                            }
                        });
                    }
                    Err(error) => warn!(%peer, %error, "http socket conversion failed"),
                }
            }
        }
    }
}

fn run_connection(socket: TcpStream, engine: &DatabaseEngine) -> std::io::Result<()> {
    socket.set_nodelay(true).ok();
    let mut reader = BufReader::new(socket.try_clone()?);
    let mut writer = socket;

    loop {
        let mut request_line = String::new();
        if reader.read_line(&mut request_line)? == 0 {
            return Ok(());
        }
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_owned();
        let path = parts.next().unwrap_or_default().to_owned();

        let mut content_length = 0_usize;
        let mut keep_alive = true;
        for _ in 0..MAX_HEADER_LINES {
            let mut header = String::new();
            if reader.read_line(&mut header)? == 0 {
                return Ok(());
            }
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            let Some((name, value)) = header.split_once(':') else {
                continue;
            };
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().unwrap_or(usize::MAX);
            } else if name.eq_ignore_ascii_case("connection") && value.eq_ignore_ascii_case("close")
            {
                keep_alive = false;
            }
        }

        if content_length > MAX_BODY_BYTES {
            respond(
                &mut writer,
                413,
                "Payload Too Large",
                r#"{"error":{"code":"frame_too_large","message":"request body is too large"}}"#,
            )?;
            return Ok(());
        }
        let mut body = vec![0_u8; content_length];
        reader.read_exact(&mut body)?;

        match (method.as_str(), path.as_str()) {
            ("OPTIONS", _) => {
                respond(&mut writer, 204, "No Content", "")?;
            }
            ("POST", "/v1/execute") => {
                let reply = execute_reply(engine, &body);
                respond(&mut writer, reply.0, reply.1, &reply.2)?;
            }
            _ => {
                respond(
                    &mut writer,
                    404,
                    "Not Found",
                    r#"{"error":{"code":"invalid_json","message":"the only endpoint is POST /v1/execute"}}"#,
                )?;
            }
        }
        if !keep_alive {
            return Ok(());
        }
    }
}

fn execute_reply(engine: &DatabaseEngine, body: &[u8]) -> (u16, &'static str, String) {
    let request: ExecuteBody = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(error) => {
            return (
                400,
                "Bad Request",
                format!(
                    r#"{{"error":{{"code":"invalid_json","message":"body must be JSON with a sql field: {error}"}}}}"#
                ),
            );
        }
    };

    let mut session = engine.session();
    match session.execute(&request.sql) {
        Ok(outputs) => {
            let results: Vec<StatementResult> = outputs.into_iter().map(map_output).collect();
            match serde_json::to_string(&serde_json::json!({ "results": results })) {
                Ok(json) => (200, "OK", json),
                Err(error) => (
                    500,
                    "Internal Server Error",
                    format!(
                        r#"{{"error":{{"code":"invalid_json","message":"encoding failed: {error}"}}}}"#
                    ),
                ),
            }
        }
        Err(error) => {
            let ProtocolResponse::Error { error } = map_engine_error(error) else {
                return (
                    500,
                    "Internal Server Error",
                    r#"{"error":{"code":"execution_error","message":"unexpected mapping"}}"#
                        .to_owned(),
                );
            };
            let json = serde_json::to_string(&serde_json::json!({ "error": error }))
                .unwrap_or_else(|_| {
                    r#"{"error":{"code":"execution_error","message":"encoding failed"}}"#.to_owned()
                });
            (400, "Bad Request", json)
        }
    }
}

fn respond(writer: &mut TcpStream, status: u16, reason: &str, body: &str) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         \r\n{body}",
        body.len(),
    );
    writer.write_all(response.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quantadb_mvcc::MvccOptions;
    use tempfile::tempdir;

    fn request(address: std::net::SocketAddr, raw: &str) -> String {
        let mut stream = TcpStream::connect(address).expect("connect");
        stream.write_all(raw.as_bytes()).expect("send");
        let mut response = String::new();
        let mut reader = BufReader::new(stream);
        // Read status line and headers.
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).expect("header line");
            let done = line == "\r\n";
            response.push_str(&line);
            if done {
                break;
            }
        }
        let length = response
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|value| value.trim().parse::<usize>().unwrap_or(0))
            })
            .unwrap_or(0);
        let mut body = vec![0_u8; length];
        reader.read_exact(&mut body).expect("body");
        response.push_str(&String::from_utf8_lossy(&body));
        response
    }

    #[tokio::test]
    async fn executes_sql_over_http_with_cors() {
        let directory = tempdir().expect("tempdir");
        let engine =
            DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        tokio::spawn(serve_http(listener, engine, 8, std::future::pending()));

        let exchange = tokio::task::spawn_blocking(move || {
            let body = r#"{"sql":"CREATE TABLE hits (id BIGINT PRIMARY KEY, page TEXT NOT NULL); INSERT INTO hits (id, page) VALUES (1, '/'); SELECT id, page FROM hits"}"#;
            let raw = format!(
                "POST /v1/execute HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len(),
            );
            let response = request(address, &raw);
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
            assert!(
                response.contains("Access-Control-Allow-Origin: *"),
                "{response}"
            );
            assert!(response.contains(r#""tag":"CREATE TABLE""#), "{response}");
            assert!(response.contains(r#""rows":[["#), "{response}");

            let bad = r#"{"sql":"SELECT nope FROM"}"#;
            let raw = format!(
                "POST /v1/execute HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{bad}",
                bad.len(),
            );
            let response = request(address, &raw);
            assert!(response.starts_with("HTTP/1.1 400"), "{response}");
            assert!(response.contains("syntax_error"), "{response}");

            let preflight = "OPTIONS /v1/execute HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
            let response = request(address, preflight);
            assert!(response.starts_with("HTTP/1.1 204"), "{response}");

            let missing = "GET /elsewhere HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
            let response = request(address, missing);
            assert!(response.starts_with("HTTP/1.1 404"), "{response}");
        });
        exchange.await.expect("client thread");
    }
}
