use quantadb_engine::DatabaseEngine;
use quantadb_mvcc::MvccOptions;
use quantadb_server::{EngineService, QuantaServer, Result, ServerConfig};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    initialize_tracing();

    let config = ServerConfig::from_env()?;
    let engine = DatabaseEngine::open(&config.data_directory, MvccOptions::default())?;
    let pg_listen = config.pg_listen_address;
    let http_listen = config.http_listen_address;
    let max_connections = config.max_connections;
    let engine_for_http = engine.clone();
    let server = QuantaServer::with_service(config, EngineService::new(engine.clone()))?;
    let listener = server.bind().await?;
    let address = listener.local_addr()?;
    info!(%address, "starting QuantaDB");

    let shutdown = wait_for_ctrl_c();
    if let Some(pg_address) = pg_listen {
        let pg_listener = tokio::net::TcpListener::bind(pg_address).await?;
        info!(address = %pg_address, "postgres protocol listening");
        tokio::spawn(quantadb_server::pg::serve_postgres(
            pg_listener,
            engine,
            max_connections,
            std::future::pending(),
        ));
    }

    if let Some(http_address) = http_listen {
        let http_listener = tokio::net::TcpListener::bind(http_address).await?;
        info!(address = %http_address, "http bridge listening");
        tokio::spawn(quantadb_server::http::serve_http(
            http_listener,
            engine_for_http,
            max_connections,
            std::future::pending(),
        ));
    }

    server.serve_until(listener, shutdown).await
}

async fn wait_for_ctrl_c() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(%error, "failed to install Ctrl+C handler");
    }
}

fn initialize_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}
