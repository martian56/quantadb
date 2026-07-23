use crate::{
    protocol::{ErrorCode, RequestFrame, ResponseFrame, PROTOCOL_VERSION},
    RequestService, RequestSession, Result, ServerConfig, ServerError, SyntaxService,
};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::timeout,
};
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct QuantaServer {
    config: Arc<ServerConfig>,
    service: Arc<dyn RequestService>,
}

impl QuantaServer {
    pub fn new(config: ServerConfig) -> Result<Self> {
        Self::with_service(config, SyntaxService)
    }

    pub fn with_service(config: ServerConfig, service: impl RequestService) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
            service: Arc::new(service),
        })
    }

    pub async fn bind(&self) -> Result<TcpListener> {
        Ok(TcpListener::bind(self.config.listen_address).await?)
    }

    /// Serve connections until `shutdown` resolves, then drain active clients.
    pub async fn serve_until<F>(&self, listener: TcpListener, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()> + Send,
    {
        let address = listener.local_addr()?;
        let connection_slots = Arc::new(Semaphore::new(self.config.max_connections));
        let request_slots = Arc::new(Semaphore::new(self.config.max_in_flight_requests));
        let (shutdown_sender, _) = watch::channel(false);
        let mut tasks = JoinSet::new();
        tokio::pin!(shutdown);

        info!(
            %address,
            max_connections = self.config.max_connections,
            max_frame_bytes = self.config.max_frame_bytes,
            "QuantaDB server listening"
        );

        loop {
            tokio::select! {
                () = &mut shutdown => {
                    info!("server shutdown requested");
                    break;
                }
                accepted = listener.accept() => {
                    let (stream, peer_address) = accepted?;
                    match Arc::clone(&connection_slots).try_acquire_owned() {
                        Ok(permit) => {
                            let config = Arc::clone(&self.config);
                            let service = Arc::clone(&self.service);
                            let request_slots = Arc::clone(&request_slots);
                            let shutdown_receiver = shutdown_sender.subscribe();
                            tasks.spawn(async move {
                                if let Err(error) = handle_connection(
                                    stream,
                                    peer_address,
                                    config,
                                    service,
                                    request_slots,
                                    shutdown_receiver,
                                    permit,
                                ).await {
                                    debug!(%peer_address, %error, "connection closed with error");
                                }
                            });
                        }
                        Err(_) => {
                            warn!(%peer_address, "connection rejected: server is at capacity");
                            reject_busy_connection(stream).await;
                        }
                    }
                }
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(error)) = completed {
                        warn!(%error, "connection task failed");
                    }
                }
            }
        }

        let _ = shutdown_sender.send(true);
        let drain = async {
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    warn!(%error, "connection task failed during shutdown");
                }
            }
        };
        if timeout(self.config.shutdown_grace, drain).await.is_err() {
            warn!("shutdown grace period expired; aborting remaining connections");
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
        info!("QuantaDB server stopped");
        Ok(())
    }
}

async fn handle_connection(
    stream: TcpStream,
    peer_address: SocketAddr,
    config: Arc<ServerConfig>,
    service: Arc<dyn RequestService>,
    request_slots: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
    _permit: OwnedSemaphorePermit,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let session = Arc::new(Mutex::new(service.open_session()));
    debug!(%peer_address, "connection accepted");

    loop {
        let frame = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            result = timeout(
                config.idle_timeout,
                read_bounded_frame(&mut reader, config.max_frame_bytes),
            ) => {
                match result {
                    Err(_) => {
                        write_response(
                            &mut writer,
                            &ResponseFrame::error(
                                0,
                                ErrorCode::IdleTimeout,
                                "connection idle timeout expired",
                                None,
                            ),
                        ).await?;
                        return Ok(());
                    }
                    Ok(Err(FrameReadError::TooLarge)) => {
                        write_response(
                            &mut writer,
                            &ResponseFrame::error(
                                0,
                                ErrorCode::FrameTooLarge,
                                format!("request frame exceeds {} bytes", config.max_frame_bytes),
                                None,
                            ),
                        ).await?;
                        return Ok(());
                    }
                    Ok(Err(FrameReadError::Io(error))) => return Err(error.into()),
                    Ok(Ok(None)) => return Ok(()),
                    Ok(Ok(Some(frame))) => frame,
                }
            }
        };

        let response = dispatch_frame(&frame, &session, &request_slots).await?;
        write_response(&mut writer, &response).await?;
    }
}

async fn reject_busy_connection(mut stream: TcpStream) {
    let response = ResponseFrame::error(
        0,
        ErrorCode::ServerBusy,
        "server connection limit reached",
        None,
    );
    let _ = write_response(&mut stream, &response).await;
    let _ = stream.shutdown().await;
}

async fn dispatch_frame(
    frame: &[u8],
    session: &Arc<Mutex<Box<dyn RequestSession>>>,
    request_slots: &Arc<Semaphore>,
) -> Result<ResponseFrame> {
    let request = match serde_json::from_slice::<RequestFrame>(frame) {
        Ok(request) => request,
        Err(error) => {
            return Ok(ResponseFrame::error(
                0,
                ErrorCode::InvalidJson,
                format!("invalid request JSON: {error}"),
                None,
            ));
        }
    };

    if request.protocol_version != PROTOCOL_VERSION {
        return Ok(ResponseFrame::error(
            request.request_id,
            ErrorCode::UnsupportedProtocolVersion,
            format!(
                "unsupported protocol version {}; server requires {}",
                request.protocol_version, PROTOCOL_VERSION
            ),
            None,
        ));
    }

    let permit = match Arc::clone(request_slots).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return Ok(ResponseFrame::error(
                request.request_id,
                ErrorCode::ServerBusy,
                "server request execution capacity reached",
                None,
            ));
        }
    };
    let session = Arc::clone(session);
    let response = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        session
            .lock()
            .map_err(|_| "request session mutex is poisoned".to_owned())
            .map(|mut session| session.handle(request.request))
    })
    .await
    .map_err(|error| ServerError::ServiceTask(error.to_string()))?
    .map_err(ServerError::ServiceTask)?;
    Ok(ResponseFrame {
        protocol_version: PROTOCOL_VERSION,
        request_id: request.request_id,
        response,
    })
}

async fn write_response<W>(writer: &mut W, response: &ResponseFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes = serde_json::to_vec(response)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

#[derive(Debug)]
enum FrameReadError {
    Io(io::Error),
    TooLarge,
}

impl From<io::Error> for FrameReadError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Read one newline-delimited frame without ever buffering more than `maximum`.
async fn read_bounded_frame<R>(
    reader: &mut R,
    maximum: usize,
) -> std::result::Result<Option<Vec<u8>>, FrameReadError>
where
    R: AsyncBufRead + Unpin,
{
    let mut frame = Vec::with_capacity(maximum.min(8 * 1024));

    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Ok(Some(frame))
            };
        }

        let newline = available.iter().position(|byte| *byte == b'\n');
        let payload_length = newline.unwrap_or(available.len());
        if frame.len().saturating_add(payload_length) > maximum {
            let consumed = newline.map_or(available.len(), |position| position + 1);
            reader.consume(consumed);
            return Err(FrameReadError::TooLarge);
        }

        frame.extend_from_slice(&available[..payload_length]);
        let consumed = newline.map_or(available.len(), |position| position + 1);
        reader.consume(consumed);

        if newline.is_some() {
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(Some(frame));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Request, Response, PROTOCOL_VERSION};
    use quantadb_engine::DatabaseEngine;
    use quantadb_mvcc::MvccOptions;
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::Duration,
    };
    use tempfile::tempdir;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::oneshot,
    };

    fn test_config() -> ServerConfig {
        ServerConfig {
            listen_address: "127.0.0.1:0".parse().expect("test address"),
            pg_listen_address: None,
            http_listen_address: None,
            data_directory: "unused-test-data".into(),
            max_connections: 8,
            max_in_flight_requests: 8,
            max_frame_bytes: 4 * 1024,
            idle_timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(2),
        }
    }

    async fn send_request(
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
        frame: &RequestFrame,
    ) -> ResponseFrame {
        let mut bytes = serde_json::to_vec(frame).expect("serialize request");
        bytes.push(b'\n');
        writer.write_all(&bytes).await.expect("write request");
        let mut response = String::new();
        reader
            .read_line(&mut response)
            .await
            .expect("read response");
        serde_json::from_str(response.trim_end()).expect("deserialize response")
    }

    #[tokio::test]
    async fn serves_ping_parse_and_syntax_errors_on_one_connection() {
        let server = QuantaServer::new(test_config()).expect("server config");
        let listener = server.bind().await.expect("bind server");
        let address = listener.local_addr().expect("local address");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .serve_until(listener, async {
                    let _ = shutdown_receiver.await;
                })
                .await
        });

        let stream = TcpStream::connect(address).await.expect("connect");
        let (read_half, mut write_half) = stream.into_split();
        let mut read_half = BufReader::new(read_half);

        let pong = send_request(
            &mut write_half,
            &mut read_half,
            &RequestFrame {
                protocol_version: PROTOCOL_VERSION,
                request_id: 1,
                request: Request::Ping,
            },
        )
        .await;
        assert!(matches!(pong.response, Response::Pong { .. }));

        let parsed = send_request(
            &mut write_half,
            &mut read_half,
            &RequestFrame {
                protocol_version: PROTOCOL_VERSION,
                request_id: 2,
                request: Request::Parse {
                    sql: "SELECT id FROM users WHERE active = true".to_owned(),
                },
            },
        )
        .await;
        assert!(matches!(
            parsed.response,
            Response::Parsed { ref statements } if statements.len() == 1
        ));

        let invalid = send_request(
            &mut write_half,
            &mut read_half,
            &RequestFrame {
                protocol_version: PROTOCOL_VERSION,
                request_id: 3,
                request: Request::Parse {
                    sql: "SELECT FROM".to_owned(),
                },
            },
        )
        .await;
        assert!(matches!(
            invalid.response,
            Response::Error {
                error: crate::protocol::ProtocolError {
                    code: ErrorCode::SyntaxError,
                    span: Some(_),
                    ..
                }
            }
        ));

        drop(write_half);
        shutdown_sender.send(()).expect("request shutdown");
        server_task
            .await
            .expect("server task")
            .expect("server shutdown");
    }

    #[tokio::test]
    async fn bounded_reader_rejects_oversized_frames() {
        let bytes = vec![b'x'; 65];
        let mut input = BufReader::new(bytes.as_slice());
        let result = read_bounded_frame(&mut input, 64).await;
        assert!(matches!(result, Err(FrameReadError::TooLarge)));
    }

    struct CountingService {
        calls: Arc<AtomicUsize>,
    }

    impl RequestService for CountingService {
        fn open_session(&self) -> Box<dyn RequestSession> {
            Box::new(CountingSession {
                calls: Arc::clone(&self.calls),
            })
        }
    }

    struct CountingSession {
        calls: Arc<AtomicUsize>,
    }

    impl RequestSession for CountingSession {
        fn handle(&mut self, _request: Request) -> Response {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Response::Pong {
                server_version: "custom".to_owned(),
            }
        }
    }

    #[tokio::test]
    async fn dispatches_validated_requests_through_custom_service() {
        let calls = Arc::new(AtomicUsize::new(0));
        let server = QuantaServer::with_service(
            test_config(),
            CountingService {
                calls: Arc::clone(&calls),
            },
        )
        .expect("server config");
        let listener = server.bind().await.expect("bind server");
        let address = listener.local_addr().expect("local address");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .serve_until(listener, async {
                    let _ = shutdown_receiver.await;
                })
                .await
        });

        let stream = TcpStream::connect(address).await.expect("connect");
        let (read_half, mut write_half) = stream.into_split();
        let mut read_half = BufReader::new(read_half);
        let response = send_request(
            &mut write_half,
            &mut read_half,
            &RequestFrame {
                protocol_version: PROTOCOL_VERSION,
                request_id: 77,
                request: Request::Parse {
                    sql: "the custom service owns this".to_owned(),
                },
            },
        )
        .await;
        assert!(matches!(
            response.response,
            Response::Pong { ref server_version } if server_version == "custom"
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        drop(write_half);
        shutdown_sender.send(()).expect("request shutdown");
        server_task
            .await
            .expect("server task")
            .expect("server shutdown");
    }

    struct BlockingService {
        started: Arc<AtomicBool>,
        released: Arc<AtomicBool>,
    }

    impl RequestService for BlockingService {
        fn open_session(&self) -> Box<dyn RequestSession> {
            Box::new(BlockingSession {
                started: Arc::clone(&self.started),
                released: Arc::clone(&self.released),
            })
        }
    }

    struct BlockingSession {
        started: Arc<AtomicBool>,
        released: Arc<AtomicBool>,
    }

    impl RequestSession for BlockingSession {
        fn handle(&mut self, _request: Request) -> Response {
            self.started.store(true, Ordering::Release);
            while !self.released.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            Response::Pong {
                server_version: "released".to_owned(),
            }
        }
    }

    #[tokio::test]
    async fn rejects_work_above_the_bounded_service_capacity() {
        let started = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        let mut config = test_config();
        config.max_in_flight_requests = 1;
        let server = QuantaServer::with_service(
            config,
            BlockingService {
                started: Arc::clone(&started),
                released: Arc::clone(&released),
            },
        )
        .expect("server config");
        let listener = server.bind().await.expect("bind server");
        let address = listener.local_addr().expect("local address");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .serve_until(listener, async {
                    let _ = shutdown_receiver.await;
                })
                .await
        });

        let first = TcpStream::connect(address).await.expect("first connect");
        let (first_read, mut first_write) = first.into_split();
        let mut first_read = BufReader::new(first_read);
        let mut bytes = serde_json::to_vec(&RequestFrame {
            protocol_version: PROTOCOL_VERSION,
            request_id: 1,
            request: Request::Ping,
        })
        .expect("serialize");
        bytes.push(b'\n');
        first_write.write_all(&bytes).await.expect("start request");
        timeout(Duration::from_secs(2), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("service should start");

        let second = TcpStream::connect(address).await.expect("second connect");
        let (second_read, mut second_write) = second.into_split();
        let mut second_read = BufReader::new(second_read);
        let busy = send_request(
            &mut second_write,
            &mut second_read,
            &RequestFrame {
                protocol_version: PROTOCOL_VERSION,
                request_id: 2,
                request: Request::Ping,
            },
        )
        .await;
        assert!(matches!(
            busy.response,
            Response::Error {
                error: crate::protocol::ProtocolError {
                    code: ErrorCode::ServerBusy,
                    ..
                }
            }
        ));

        released.store(true, Ordering::Release);
        let mut first_response = String::new();
        first_read
            .read_line(&mut first_response)
            .await
            .expect("read released response");
        assert!(matches!(
            serde_json::from_str::<ResponseFrame>(first_response.trim_end())
                .expect("decode response")
                .response,
            Response::Pong { .. }
        ));

        drop(first_write);
        drop(second_write);
        shutdown_sender.send(()).expect("request shutdown");
        server_task
            .await
            .expect("server task")
            .expect("server shutdown");
    }

    #[tokio::test]
    async fn engine_sessions_preserve_transaction_state_across_frames() {
        let directory = tempdir().expect("tempdir");
        let engine =
            DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
        let server = QuantaServer::with_service(test_config(), crate::EngineService::new(engine))
            .expect("server");
        let listener = server.bind().await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .serve_until(listener, async {
                    let _ = shutdown_receiver.await;
                })
                .await
        });

        let first = TcpStream::connect(address).await.expect("first connection");
        let (first_read, mut first_write) = first.into_split();
        let mut first_read = BufReader::new(first_read);
        for (request_id, sql) in [
            (1, "BEGIN"),
            (
                2,
                "CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)",
            ),
            (3, "INSERT INTO accounts VALUES (1, 100), (2, 250)"),
        ] {
            let response = send_request(
                &mut first_write,
                &mut first_read,
                &RequestFrame {
                    protocol_version: PROTOCOL_VERSION,
                    request_id,
                    request: Request::Execute {
                        sql: sql.to_owned(),
                    },
                },
            )
            .await;
            assert!(
                !matches!(response.response, Response::Error { .. }),
                "{response:?}"
            );
        }

        let second = TcpStream::connect(address)
            .await
            .expect("second connection");
        let (second_read, mut second_write) = second.into_split();
        let mut second_read = BufReader::new(second_read);
        let invisible = send_request(
            &mut second_write,
            &mut second_read,
            &RequestFrame {
                protocol_version: PROTOCOL_VERSION,
                request_id: 4,
                request: Request::Execute {
                    sql: "SELECT * FROM accounts".to_owned(),
                },
            },
        )
        .await;
        assert!(matches!(
            invisible.response,
            Response::Error {
                error: crate::protocol::ProtocolError {
                    code: ErrorCode::ExecutionError,
                    ..
                }
            }
        ));

        send_request(
            &mut first_write,
            &mut first_read,
            &RequestFrame {
                protocol_version: PROTOCOL_VERSION,
                request_id: 5,
                request: Request::Execute {
                    sql: "COMMIT".to_owned(),
                },
            },
        )
        .await;
        let visible = send_request(
            &mut second_write,
            &mut second_read,
            &RequestFrame {
                protocol_version: PROTOCOL_VERSION,
                request_id: 6,
                request: Request::Execute {
                    sql: "SELECT id, balance FROM accounts WHERE balance >= 200".to_owned(),
                },
            },
        )
        .await;
        assert!(matches!(
            visible.response,
            Response::Executed { ref results }
                if matches!(
                    results.as_slice(),
                    [crate::protocol::StatementResult::Query { rows, .. }]
                        if rows == &vec![vec![
                            crate::protocol::ScalarValue::Integer(2),
                            crate::protocol::ScalarValue::Integer(250),
                        ]]
                )
        ));

        drop(first_write);
        drop(second_write);
        shutdown_sender.send(()).expect("shutdown");
        server_task
            .await
            .expect("server task")
            .expect("server shutdown");
    }
}
