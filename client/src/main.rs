// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tauri::Manager;
use std::sync::Mutex;

mod client;
mod error;

use client::QuantaClient;
use error::QuantaClientError;

struct AppState {
    client: Mutex<Option<QuantaClient>>,
}

#[tauri::command]
async fn connect_to_server(
    state: tauri::State<'_, AppState>,
    address: String,
) -> Result<String, String> {
    let mut client_guard = state.client.lock().unwrap();
    let mut client = QuantaClient::new(&address);
    
    client.connect().await
        .map_err(|e| format!("Failed to connect: {}", e))?;
    
    *client_guard = Some(client);
    Ok("Connected successfully".to_string())
}

#[tauri::command]
async fn execute_query(
    state: tauri::State<'_, AppState>,
    query: String,
) -> Result<serde_json::Value, String> {
    let mut client_guard = state.client.lock().unwrap();
    
    if let Some(ref mut client) = *client_guard {
        let result = client.execute(&query).await
            .map_err(|e| format!("Query execution failed: {}", e))?;
        
        serde_json::to_value(result)
            .map_err(|e| format!("Serialization error: {}", e))
    } else {
        Err("Not connected to server".to_string())
    }
}

#[tauri::command]
async fn disconnect_from_server(
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    let mut client_guard = state.client.lock().unwrap();
    
    if let Some(ref mut client) = *client_guard {
        client.disconnect().await
            .map_err(|e| format!("Failed to disconnect: {}", e))?;
        *client_guard = None;
        Ok("Disconnected successfully".to_string())
    } else {
        Ok("Already disconnected".to_string())
    }
}

#[tauri::command]
fn is_connected(state: tauri::State<'_, AppState>) -> bool {
    let client_guard = state.client.lock().unwrap();
    client_guard.as_ref().map_or(false, |client| client.is_connected())
}

fn main() {
    tauri::Builder::default()
        .manage(AppState {
            client: Mutex::new(None),
        })
        .invoke_handler(tauri::generate_handler![
            connect_to_server,
            execute_query,
            disconnect_from_server,
            is_connected
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
