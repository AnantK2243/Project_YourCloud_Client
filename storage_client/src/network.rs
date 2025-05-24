// src/network.rs

use crate::config::{Config, save_config};
use crate::commands::BackendCommand;
use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use serde_json;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use sysinfo::System;
use std::path::PathBuf;


// Simplified response structures
#[derive(Serialize)]
struct CommandResult<'a> {
    command_id: &'a str,
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct StatusReport<'a, S: Serialize> {
    command_id: &'a str,
    status: S,
}


pub struct Network {
    backend_ws_url: String,
    auth_token: String,
    node_id: String,
    command_sender: mpsc::Sender<BackendCommand>, // Corrected type
}

impl Network {
    pub fn init(
        backend_ws_url: String,
        auth_token: String,
        node_id: String,
        command_sender: mpsc::Sender<BackendCommand>, // Corrected type
    ) -> Self {
        Self {
            backend_ws_url,
            auth_token,
            node_id,
            command_sender,
        }
    }

    pub async fn run_connection_loop(&self, mut outgoing_responses_rx: mpsc::Receiver<Message>) -> Result<()> {
        let mut attempts = 0;
        let base_delay_ms = 1000;
        let max_delay_ms = 60000; // 1 minute
        const MAX_CONNECTION_ATTEMPTS: u32 = 10;

        loop {
            info!("Attempting to connect to WebSocket: {}. Attempt #{}", self.backend_ws_url, attempts + 1);

            match self.establish_and_process_messages(&mut outgoing_responses_rx).await {
                Ok(_) => {
                    info!("WebSocket connection closed gracefully or stream ended. Re-attempting connection.");
                    attempts = 0; // Reset attempts on graceful close before retry
                }
                Err(e) => {
                    error!("WebSocket connection error: {:#}", e);
                    attempts += 1;
                    if attempts >= MAX_CONNECTION_ATTEMPTS {
                        error!("Maximum reconnection attempts ({}) reached. Giving up.", MAX_CONNECTION_ATTEMPTS);
                        return Err(e.context(format!("Failed to connect after {} attempts", MAX_CONNECTION_ATTEMPTS)));
                    }
                }
            }

            let delay_ms = std::cmp::min(base_delay_ms * 2_u64.pow(attempts.saturating_sub(1)), max_delay_ms);
            info!("Retrying connection in {}ms...", delay_ms);
            sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    async fn establish_and_process_messages(&self, outgoing_responses_rx: &mut mpsc::Receiver<Message>) -> Result<()> {
        let (ws_stream, _) = connect_async(&self.backend_ws_url)
            .await
            .with_context(|| format!("Failed to connect to backend WebSocket: {}", self.backend_ws_url))?;
        info!("Successfully connected to backend WebSocket: {}", self.backend_ws_url);

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        // Authentication
        let auth_payload = serde_json::json!({
            "type": "AUTH",
            "node_id": self.node_id,
            "token": self.auth_token,
        });
        ws_sender
            .send(Message::Text(auth_payload.to_string()))
            .await
            .context("Failed to send authentication message")?;
        info!("Authentication message sent.");

        const AUTH_TIMEOUT_SECONDS: u64 = 10;
        match timeout(Duration::from_secs(AUTH_TIMEOUT_SECONDS), ws_receiver.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                match serde_json::from_str::<serde_json::Value>(&text) { // Use Value for flexible auth response
                    Ok(response) => {
                        if response.get("type").and_then(serde_json::Value::as_str) == Some("AUTH_SUCCESS") {
                            info!("Authentication successful.");
                        } else {
                            return Err(anyhow!("Authentication failed: {:?}", response));
                        }
                    }
                    Err(e) => return Err(anyhow!(e)).context("Failed to parse auth response JSON"),
                }
            }
            Ok(Some(Ok(other_msg))) => return Err(anyhow!("Unexpected message type during auth: {:?}", other_msg)),
            Ok(Some(Err(e))) => return Err(anyhow::Error::from(e)).context("Error waiting for auth response"),
            Ok(None) => return Err(anyhow!("Connection closed by peer before auth response")),
            Err(_) => return Err(anyhow!("Timed out waiting for auth response after {} seconds", AUTH_TIMEOUT_SECONDS)),
        }
        
        // Message Handling Loop
        loop {
            tokio::select! {
                Some(message_result) = ws_receiver.next() => {
                    match message_result {
                        Ok(message) => {
                            match message {
                                Message::Text(text) => {
                                    if let Err(e) = self.process_message(text).await {
                                        error!("Error processing incoming message: {}", e);
                                        // Decide if this error should terminate the connection or just log
                                    }
                                }
                                Message::Binary(_) => warn!("Received binary message, which is not currently processed."),
                                Message::Ping(ping_data) => {
                                    info!("Received Ping, sending Pong.");
                                    if let Err(e) = ws_sender.send(Message::Pong(ping_data)).await {
                                        error!("Failed to send Pong: {}", e);
                                        return Err(e.into()); // Critical error, terminate
                                    }
                                }
                                Message::Pong(_) => info!("Received Pong from server."),
                                Message::Close(close_frame) => {
                                    info!("WebSocket connection closed by peer: {:?}", close_frame);
                                    return Ok(()); // Graceful close, will trigger reconnect by run_connection_loop
                                }
                                Message::Frame(_) => { /* Usually not handled at this level */ }
                            }
                        }
                        Err(e) => {
                            error!("WebSocket receive error: {}", e);
                            return Err(e.into()); // Propagate error to trigger reconnection logic.
                        }
                    }
                }
                Some(response_to_send) = outgoing_responses_rx.recv() => {
                    if let Err(e) = ws_sender.send(response_to_send).await {
                        error!("Failed to send outgoing response via WebSocket: {}", e);
                        return Err(e.into()); // Critical error, terminate
                    } else {
                        // info!("Successfully sent response to backend."); // Can be verbose
                    }
                }
                else => {
                    info!("WebSocket receiver stream ended or outgoing_responses_rx closed. Terminating current connection processing.");
                    return Ok(()); // Will trigger reconnect
                }
            }
        }
    }

    async fn process_message(&self, message_text: String) -> Result<()> {
        // info!("Processing message: {}", message_text); // Can be verbose
        match serde_json::from_str::<BackendCommand>(&message_text) {
            Ok(command) => {
                // info!("Parsed command: {:?}", command); // Can be verbose
                if let Err(e) = self.command_sender.send(command).await {
                    error!("Failed to send parsed command to internal channel: {}. Channel might be closed.", e);
                    // This could be a critical error if the command handler task has panicked.
                    // Depending on design, might want to return Err(e.into()) here.
                }
            }
            Err(e) => {
                error!("Failed to parse incoming message as BackendCommand: {}. Message: '{}'", e, message_text);
                // Send an Unknown command to the handler to acknowledge receipt but indicate parsing failure.
                let unknown_command = BackendCommand::Unknown; // Assuming BackendCommand has an Unknown variant
                if let Err(send_err) = self.command_sender.send(unknown_command).await {
                    error!("Failed to send Unknown command to internal channel: {}", send_err);
                }
            }
        }
        Ok(())
    }
}

// Define structs for registration request and response
#[derive(Serialize)]
struct RegistrationRequest {
    requested_max_gib: u64,
    system_info: SystemInfo,
}

#[derive(Serialize)]
struct SystemInfo {
    hostname: String,
    os_version: String,
}

#[derive(Deserialize)]
struct RegistrationResponse {
    node_id: String,
    auth_token: String,
}

async fn register_node(config: &mut Config, http_client: &HttpClient) -> Result<()> {
    info!("Node ID or Auth Token is missing. Starting registration process.");

    let mut sys = System::new_all();
    sys.refresh_all();

    let system_info = SystemInfo {
        hostname: System::host_name().unwrap_or_else(|| "unknown_hostname".to_string()),
        os_version: System::long_os_version().unwrap_or_else(|| "unknown_os_version".to_string()),
    };

    let registration_payload = RegistrationRequest {
        requested_max_gib: config.max_storage_gib,
        system_info,
    };

    let registration_url = format!("{}/register_node", config.backend_api_url);
    info!("Registering node with backend: {}", registration_url);

    let response = http_client
        .post(&registration_url)
        .json(&registration_payload)
        .send()
        .await
        .context("Failed to send registration request to backend")?;

    let status = response.status();
    if status.is_success() {
        let reg_response: RegistrationResponse = response
            .json()
            .await
            .context("Failed to parse registration response JSON")?;
        info!("Successfully registered with backend. Node ID: {}", reg_response.node_id);
        config.node_id = reg_response.node_id;
        config.auth_token = reg_response.auth_token;
        save_config(config).await.context("Failed to save updated config after registration")?;
        info!("Configuration updated with new Node ID and Auth Token.");
        Ok(())
    } else {
        let error_body = response.text().await.unwrap_or_else(|_| "No error body".to_string());
        error!("Registration failed. Status: {}. Body: {}", status, error_body);
        Err(anyhow!("Registration failed with status: {}", status))
    }
}

// Public functions to be called by commands::handle_command to send responses
pub async fn send_command_result(
    response_tx: &mpsc::Sender<Message>,
    command_id: String,
    operation_result: Result<()>,
) -> Result<()> {
    let response = CommandResult {
        command_id: &command_id,
        success: operation_result.is_ok(),
        error: operation_result.err().map(|e| e.to_string()),
    };

    let response_text = serde_json::to_string(&response)
        .context("Failed to serialize command result")?;
    
    response_tx.send(Message::Text(response_text)).await
        .context("Failed to send command result")?;
    Ok(())
}

pub async fn send_status_report<S: Serialize>(
    response_tx: &mpsc::Sender<Message>,
    command_id: String,
    status_data: S,
) -> Result<()> {
    let response = StatusReport {
        command_id: &command_id,
        status: status_data,
    };
    
    let response_text = serde_json::to_string(&response)
        .context("Failed to serialize status report")?;
    
    response_tx.send(Message::Text(response_text)).await
        .context("Failed to send status report")?;
    Ok(())
}

pub async fn start_communication_loop(initial_config: &Config, used_space_bytes: Arc<AtomicU64>, max_storage_bytes: Arc<AtomicU64>, chunk_count: Arc<AtomicU64>) -> Result<()> {
    let mut current_config = initial_config.clone();
    let http_client = HttpClient::new();

    if current_config.node_id.is_empty() || current_config.auth_token.is_empty() {
        register_node(&mut current_config, &http_client)
            .await
            .context("Node registration process failed")?;
    } else {
        info!("Node ID and Auth Token found in config.");
    }

    if !tokio::fs::try_exists(&current_config.storage_path).await.unwrap_or(false) {
        info!("Storage path {} does not exist. Creating it.", current_config.storage_path.display());
        fs::create_dir_all(&current_config.storage_path)
            .await
            .with_context(|| format!("Failed to create storage directory: {}", current_config.storage_path.display()))?;
        info!("Storage path {} created.", current_config.storage_path.display());
    } else {
        info!("Storage path {} already exists.", current_config.storage_path.display());
    }

    let (backend_command_tx, mut backend_command_rx) = mpsc::channel::<BackendCommand>(32);
    let (outgoing_responses_tx, outgoing_responses_rx) = mpsc::channel::<Message>(32);

    let network = Network::init(
        current_config.backend_ws_url.clone(),
        current_config.auth_token.clone(),
        current_config.node_id.clone(),
        backend_command_tx,
    );

    let storage_path_clone: PathBuf = current_config.storage_path.clone();
    let responses_tx_clone = outgoing_responses_tx.clone();
    let handler_used_space = used_space_bytes.clone();
    let handler_max_space = max_storage_bytes.clone();
    let handler_chunk_count = chunk_count.clone();

    tokio::spawn(async move {
        info!("Command handler task started.");
        while let Some(command) = backend_command_rx.recv().await {
            // info!("Command handler received: {:?}", command); // Can be verbose
            if let Err(e) = crate::commands::handle_command(
                command,
                storage_path_clone.clone(),
                responses_tx_clone.clone(),
                handler_used_space.clone(),
                handler_max_space.clone(),
                handler_chunk_count.clone(),
            )
            .await
            {
                error!("Error handling command: {:?}", e);
            }
        }
        info!("Command handler task finished (backend_command_rx channel closed).");
    });

    network.run_connection_loop(outgoing_responses_rx).await
}

