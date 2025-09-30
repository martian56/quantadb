use crate::sql::{SqlParser, QueryExecutor};
use crate::net::protocol::{QuantaProtocol, QuantaResponse};
use crate::error::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn, error, debug};
use std::path::PathBuf;

pub struct QuantaServer {
    parser: SqlParser,
    executor: Arc<Mutex<QueryExecutor>>,
}

impl QuantaServer {
    pub fn new() -> Self {
        Self {
            parser: SqlParser::new(),
            executor: Arc::new(Mutex::new(QueryExecutor::new())),
        }
    }

    pub fn new_with_persistence(data_dir: PathBuf) -> Result<Self> {
        Ok(Self {
            parser: SqlParser::new(),
            executor: Arc::new(Mutex::new(QueryExecutor::new_with_persistence(data_dir)?)),
        })
    }

    pub async fn start(&self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("📡 QuantaDB server listening on {}", addr);
        info!("🔗 Ready to accept connections");

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("🔌 New connection from {}", addr);
                    
                    let executor = Arc::clone(&self.executor);
                    let parser = SqlParser::new();
                    
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_client(stream, parser, executor, addr).await {
                            error!("❌ Error handling client {}: {}", addr, e);
                        } else {
                            info!("✅ Client {} disconnected", addr);
                        }
                    });
                }
                Err(e) => {
                    error!("❌ Failed to accept connection: {}", e);
                }
            }
        }
    }

    async fn handle_client(
        stream: TcpStream,
        parser: SqlParser,
        executor: Arc<Mutex<QueryExecutor>>,
        client_addr: std::net::SocketAddr,
    ) -> Result<()> {
        let (reader, mut writer) = stream.into_split();
        let reader = BufReader::new(reader);
        let mut lines = reader.lines();

        // Send welcome message
        let welcome = QuantaResponse::success(
            "Welcome to QuantaDB! Send SQL queries to get started.".to_string(),
            None,
        );
        let welcome_json = QuantaProtocol::serialize_response(&welcome)?;
        writer.write_all(format!("{}\n", welcome_json).as_bytes()).await?;
        writer.flush().await?;
        debug!("📤 Sent welcome message to {}", client_addr);

        while let Some(line) = lines.next_line().await? {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            debug!("📥 Received query from {}: {}", client_addr, line);

            // Parse the request to extract the SQL query
            let sql_query = match QuantaProtocol::parse_request(line) {
                Ok(request) => request.query,
                Err(e) => {
                    warn!("❌ Failed to parse request from {}: {}", client_addr, e);
                    let error_response = QuantaResponse::error(format!("Invalid request format: {}", e));
                    let error_json = QuantaProtocol::serialize_response(&error_response)?;
                    writer.write_all(format!("{}\n", error_json).as_bytes()).await?;
                    writer.flush().await?;
                    continue;
                }
            };

            let response = match Self::process_query(&sql_query, &parser, &executor).await {
                Ok(result) => {
                    let data = if let Some(rows) = result.data {
                        Some(serde_json::to_value(rows)?)
                    } else {
                        None
                    };
                    debug!("✅ Query executed successfully for {}", client_addr);
                    QuantaResponse::success(result.message, data)
                }
                Err(e) => {
                    warn!("❌ Query failed for {}: {}", client_addr, e);
                    QuantaResponse::error(e.to_string())
                }
            };

            let response_json = QuantaProtocol::serialize_response(&response)?;
            writer.write_all(format!("{}\n", response_json).as_bytes()).await?;
            writer.flush().await?;
            debug!("📤 Sent response to {}", client_addr);
        }

        Ok(())
    }

    async fn process_query(
        query: &str,
        parser: &SqlParser,
        executor: &Arc<Mutex<QueryExecutor>>,
    ) -> Result<crate::sql::executor::QueryResult> {
        // Parse the SQL query
        let statement = parser.parse(query)?;
        
        // Execute the query
        let exec = executor.lock().await;
        exec.execute(statement)
    }
}
