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
    let server = QuantaServer::with_service(config, EngineService::new(engine))?;
    let listener = server.bind().await?;
    let address = listener.local_addr()?;
    info!(%address, "starting QuantaDB");

    server
        .serve_until(listener, async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                error!(%error, "failed to install Ctrl+C handler");
            }
        })
        .await
}

fn initialize_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}
