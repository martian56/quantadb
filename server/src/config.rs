use crate::{Result, ServerError};
use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

const DEFAULT_MAX_CONNECTIONS: usize = 1_024;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 256;
const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;
const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 300;
const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub listen_address: SocketAddr,
    /// Where the PostgreSQL wire protocol listens; None disables it.
    pub pg_listen_address: Option<SocketAddr>,
    pub data_directory: PathBuf,
    pub max_connections: usize,
    pub max_in_flight_requests: usize,
    pub max_frame_bytes: usize,
    pub idle_timeout: Duration,
    pub shutdown_grace: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_address: SocketAddr::from(([127, 0, 0, 1], 54_321)),
            pg_listen_address: Some(SocketAddr::from(([127, 0, 0, 1], 55_432))),
            data_directory: PathBuf::from("quantadb-data"),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_in_flight_requests: DEFAULT_MAX_IN_FLIGHT_REQUESTS,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            idle_timeout: Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
            shutdown_grace: Duration::from_secs(DEFAULT_SHUTDOWN_GRACE_SECS),
        }
    }
}

impl ServerConfig {
    /// Load server settings from `QUANTA_*` environment variables.
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();

        if let Some(value) = read_env("QUANTA_LISTEN_ADDRESS")? {
            config.listen_address = value.parse().map_err(|error| {
                ServerError::Configuration(format!(
                    "QUANTA_LISTEN_ADDRESS must be an IP socket address: {error}"
                ))
            })?;
        }
        if let Some(value) = read_env("QUANTA_PG_LISTEN_ADDRESS")? {
            config.pg_listen_address = if value.eq_ignore_ascii_case("off") {
                None
            } else {
                Some(value.parse().map_err(|error| {
                    ServerError::Configuration(format!(
                        "QUANTA_PG_LISTEN_ADDRESS must be an IP socket address or \"off\": {error}"
                    ))
                })?)
            };
        }
        if let Some(value) = read_env("QUANTA_DATA_DIR")? {
            config.data_directory = PathBuf::from(value);
        }
        if let Some(value) = read_env("QUANTA_MAX_CONNECTIONS")? {
            config.max_connections = parse_number("QUANTA_MAX_CONNECTIONS", &value)?;
        }
        if let Some(value) = read_env("QUANTA_MAX_IN_FLIGHT_REQUESTS")? {
            config.max_in_flight_requests = parse_number("QUANTA_MAX_IN_FLIGHT_REQUESTS", &value)?;
        }
        if let Some(value) = read_env("QUANTA_MAX_FRAME_BYTES")? {
            config.max_frame_bytes = parse_number("QUANTA_MAX_FRAME_BYTES", &value)?;
        }
        if let Some(value) = read_env("QUANTA_IDLE_TIMEOUT_SECS")? {
            config.idle_timeout =
                Duration::from_secs(parse_number("QUANTA_IDLE_TIMEOUT_SECS", &value)?);
        }
        if let Some(value) = read_env("QUANTA_SHUTDOWN_GRACE_SECS")? {
            config.shutdown_grace =
                Duration::from_secs(parse_number("QUANTA_SHUTDOWN_GRACE_SECS", &value)?);
        }

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_connections == 0 {
            return Err(ServerError::Configuration(
                "max_connections must be greater than zero".to_owned(),
            ));
        }
        if self.data_directory.as_os_str().is_empty() {
            return Err(ServerError::Configuration(
                "data_directory cannot be empty".to_owned(),
            ));
        }
        if self.max_in_flight_requests == 0 {
            return Err(ServerError::Configuration(
                "max_in_flight_requests must be greater than zero".to_owned(),
            ));
        }
        if !(256..=64 * 1024 * 1024).contains(&self.max_frame_bytes) {
            return Err(ServerError::Configuration(
                "max_frame_bytes must be between 256 bytes and 64 MiB".to_owned(),
            ));
        }
        if self.idle_timeout.is_zero() {
            return Err(ServerError::Configuration(
                "idle_timeout must be greater than zero".to_owned(),
            ));
        }
        if self.shutdown_grace.is_zero() {
            return Err(ServerError::Configuration(
                "shutdown_grace must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

fn read_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(ServerError::Configuration(format!(
            "{name} contains non-Unicode data"
        ))),
    }
}

fn parse_number<T>(name: &str, value: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|error| {
        ServerError::Configuration(format!("{name} must be a positive integer: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_resource_limits() {
        let mut config = ServerConfig {
            max_connections: 0,
            ..ServerConfig::default()
        };
        assert!(config.validate().is_err());

        config.max_connections = 1;
        config.max_in_flight_requests = 0;
        assert!(config.validate().is_err());

        config.max_in_flight_requests = 1;
        config.max_frame_bytes = 255;
        assert!(config.validate().is_err());

        config.max_frame_bytes = 256;
        assert!(config.validate().is_ok());
    }
}
