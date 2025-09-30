use serde::{Deserialize, Serialize};
use crate::error::{QuantaError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantaRequest {
    pub query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantaResponse {
    pub success: bool,
    pub message: String,
    pub data: Option<serde_json::Value>,
    pub error: Option<String>,
}

impl QuantaResponse {
    pub fn success(message: String, data: Option<serde_json::Value>) -> Self {
        Self {
            success: true,
            message,
            data,
            error: None,
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            success: false,
            message: String::new(),
            data: None,
            error: Some(message),
        }
    }
}

pub struct QuantaProtocol;

impl QuantaProtocol {
    pub fn parse_request(data: &str) -> Result<QuantaRequest> {
        // Try JSON first
        if let Ok(request) = serde_json::from_str::<QuantaRequest>(data) {
            return Ok(request);
        }

        // Fallback to plain text query
        Ok(QuantaRequest {
            query: data.trim().to_string(),
        })
    }

    pub fn serialize_response(response: &QuantaResponse) -> Result<String> {
        serde_json::to_string(response)
            .map_err(|e| QuantaError::SerializationError(e))
    }

    pub fn parse_response(data: &str) -> Result<QuantaResponse> {
        serde_json::from_str(data)
            .map_err(|e| QuantaError::SerializationError(e))
    }
}
