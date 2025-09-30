use quanta_server::{net::QuantaServer, ServerConfig, Result};
use tracing::info;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    
    info!("🚀 Starting QuantaDB Server...");
    
    // Load configuration from environment variables
    let config = ServerConfig::from_env()?;
    config.validate()?;
    config.print_info();
    
    info!("💡 Environment Variables:");
    info!("   QUANTA_HOST - Server host (default: 127.0.0.1)");
    info!("   QUANTA_PORT - Server port (default: 54321)");
    info!("   QUANTA_MAX_CONNECTIONS - Max concurrent connections (default: 100)");
    info!("   QUANTA_LOG_LEVEL - Log level (default: info)");
    info!("   QUANTA_DATA_DIR - Data directory (default: ./data)");
    info!("   QUANTA_ENABLE_PERSISTENCE - Enable persistence (default: true)");
    info!("");
    
    let server = if config.enable_persistence {
        info!("💾 Starting with file-based persistence enabled");
        QuantaServer::new_with_persistence(PathBuf::from(&config.data_dir))?
    } else {
        info!("💾 Starting with in-memory storage only");
        QuantaServer::new()
    };
    let address = config.address();
    
    // Handle graceful shutdown
    let shutdown_signal = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        info!("🛑 Received shutdown signal");
    };
    
    tokio::select! {
        _ = server.start(&address) => {
            info!("Server stopped");
        }
        _ = shutdown_signal => {
            info!("🛑 Shutting down QuantaDB Server...");
        }
    }
    
    Ok(())
}
