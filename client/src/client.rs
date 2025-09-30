use crate::error::{QuantaClientError, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuantaRequest {
    query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuantaResponse {
    success: bool,
    message: String,
    data: Option<serde_json::Value>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub success: bool,
    pub message: String,
    pub data: Option<Vec<Row>>,
    pub affected_rows: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Float(f64),
    Null,
}

pub struct QuantaClient {
    stream: Option<TcpStream>,
    address: String,
}

impl QuantaClient {
    pub fn new(address: &str) -> Self {
        Self {
            stream: None,
            address: address.to_string(),
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        let stream = TcpStream::connect(&self.address).await?;
        self.stream = Some(stream);
        Ok(())
    }

    pub async fn execute(&mut self, query: &str) -> Result<QueryResult> {
        if self.stream.is_none() {
            return Err(QuantaClientError::ConnectionError("Not connected to server".to_string()));
        }

        let stream = self.stream.as_mut().unwrap();
        let (reader, writer) = stream.split();
        let mut reader = BufReader::new(reader);
        let mut writer = writer;

        // Send the query
        let request = QuantaRequest {
            query: query.to_string(),
        };
        let request_json = serde_json::to_string(&request)?;
        writer.write_all(format!("{}\n", request_json).as_bytes()).await?;
        writer.flush().await?;

        // Read the response
        let mut response_line = String::new();
        reader.read_line(&mut response_line).await?;
        let response_line = response_line.trim();

        let response: QuantaResponse = serde_json::from_str(response_line)?;

        if !response.success {
            return Err(QuantaClientError::QueryError(
                response.error.unwrap_or_else(|| "Unknown error".to_string())
            ));
        }

        // Convert the response to QueryResult
        let data = if let Some(json_data) = response.data {
            if let Ok(rows) = serde_json::from_value::<Vec<Row>>(json_data) {
                Some(rows)
            } else {
                None
            }
        } else {
            None
        };

        Ok(QueryResult {
            success: response.success,
            message: response.message,
            data,
            affected_rows: None, // TODO: Parse from response
        })
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        self.stream = None;
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }
}
