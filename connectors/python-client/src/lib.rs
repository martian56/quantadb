use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
struct Row {
    values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Float(f64),
    Null,
}

#[pyclass]
pub struct QuantaClient {
    address: String,
    stream: Option<tokio::net::TcpStream>,
}

#[pymethods]
impl QuantaClient {
    #[new]
    fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
            stream: None,
        }
    }

    fn connect(&mut self) -> PyResult<()> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let stream = tokio::net::TcpStream::connect(&self.address).await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(format!("Failed to connect: {}", e)))?;
            self.stream = Some(stream);
            Ok::<(), PyErr>(())
        })
    }

    fn execute(&mut self, query: &str) -> PyResult<PyObject> {
        if self.stream.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyConnectionError, _>("Not connected to server"));
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let stream = self.stream.as_mut().unwrap();
            let (reader, writer) = stream.split();
            let mut reader = tokio::io::BufReader::new(reader);
            let mut writer = writer;

            // Send the query
            let request = QuantaRequest {
                query: query.to_string(),
            };
            let request_json = serde_json::to_string(&request)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e)))?;
            
            writer.write_all(format!("{}\n", request_json).as_bytes()).await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write error: {}", e)))?;
            writer.flush().await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Flush error: {}", e)))?;

            // Read the response
            let mut response_line = String::new();
            reader.read_line(&mut response_line).await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read error: {}", e)))?;
            let response_line = response_line.trim();

            let response: QuantaResponse = serde_json::from_str(response_line)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Deserialization error: {}", e)))?;

            if !response.success {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    response.error.unwrap_or_else(|| "Unknown error".to_string())
                ));
            }

            // Convert to Python objects
            Python::with_gil(|py| {
                let result = PyDict::new(py);
                result.set_item("success", response.success)?;
                result.set_item("message", response.message)?;
                
                if let Some(data) = response.data {
                    if let Ok(rows) = serde_json::from_value::<Vec<Row>>(data) {
                        let py_rows = rows.into_iter().map(|row| {
                            let py_row = PyDict::new(py);
                            let py_values = row.values.into_iter().map(|value| {
                                match value {
                                    Value::Int(i) => i.into_py(py),
                                    Value::Text(s) => s.into_py(py),
                                    Value::Bool(b) => b.into_py(py),
                                    Value::Float(f) => f.into_py(py),
                                    Value::Null => py.None(),
                                }
                            }).collect::<Vec<_>>();
                            py_row.set_item("values", py_values)?;
                            Ok::<PyObject, PyErr>(py_row.into())
                        }).collect::<Result<Vec<_>, _>>()?;
                        result.set_item("data", py_rows)?;
                    }
                }
                
                Ok::<PyObject, PyErr>(result.into())
            })
        })
    }

    fn disconnect(&mut self) -> PyResult<()> {
        self.stream = None;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.stream.is_some()
    }
}

#[pymodule]
fn quanta_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<QuantaClient>()?;
    Ok(())
}
