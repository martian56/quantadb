use crate::error::{QuantaError, Result};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub log_level: String,
    pub data_dir: String,
    pub enable_persistence: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 54321, // QuantaDB's unique port
            max_connections: 100,
            log_level: "info".to_string(),
            data_dir: "./data".to_string(),
            enable_persistence: true,
        }
    }
}

impl ServerConfig {
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();
        
        // Override with environment variables if present
        if let Ok(host) = env::var("QUANTA_HOST") {
            config.host = host;
        }
        
        if let Ok(port_str) = env::var("QUANTA_PORT") {
            config.port = port_str.parse::<u16>()
                .map_err(|_| QuantaError::NetworkError("Invalid QUANTA_PORT value".to_string()))?;
        }
        
        if let Ok(max_conn_str) = env::var("QUANTA_MAX_CONNECTIONS") {
            config.max_connections = max_conn_str.parse::<usize>()
                .map_err(|_| QuantaError::NetworkError("Invalid QUANTA_MAX_CONNECTIONS value".to_string()))?;
        }
        
        if let Ok(log_level) = env::var("QUANTA_LOG_LEVEL") {
            config.log_level = log_level;
        }
        
        if let Ok(data_dir) = env::var("QUANTA_DATA_DIR") {
            config.data_dir = data_dir;
        }
        
        if let Ok(enable_persistence) = env::var("QUANTA_ENABLE_PERSISTENCE") {
            config.enable_persistence = enable_persistence.parse::<bool>()
                .map_err(|_| QuantaError::NetworkError("Invalid QUANTA_ENABLE_PERSISTENCE value".to_string()))?;
        }
        
        Ok(config)
    }
    
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
    
    pub fn validate(&self) -> Result<()> {
        if self.port == 0 {
            return Err(QuantaError::NetworkError("Port cannot be 0".to_string()));
        }
        
        if self.max_connections == 0 {
            return Err(QuantaError::NetworkError("Max connections cannot be 0".to_string()));
        }
        
        let valid_log_levels = ["error", "warn", "info", "debug", "trace"];
        if !valid_log_levels.contains(&self.log_level.as_str()) {
            return Err(QuantaError::NetworkError(
                format!("Invalid log level: {}. Valid options: {:?}", self.log_level, valid_log_levels)
            ));
        }
        
        Ok(())
    }
    
    pub fn print_info(&self) {
        println!("🔧 QuantaDB Server Configuration:");
        println!("   📍 Host: {}", self.host);
        println!("   🔌 Port: {}", self.port);
        println!("   👥 Max Connections: {}", self.max_connections);
        println!("   📝 Log Level: {}", self.log_level);
        println!("   💾 Data Directory: {}", self.data_dir);
        println!("   🔄 Persistence: {}", if self.enable_persistence { "Enabled" } else { "Disabled" });
        println!("   🌐 Address: {}", self.address());
        println!();
    }
}
