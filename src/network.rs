// src/network.rs

use crate::config::{Config, save_config};
use crate::commands::BackendCommand;
use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn, debug};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use serde_json;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, timeout};
use tokio_tungstenite::{connect_async, connect_async_tls_with_config, Connector};
use tokio_tungstenite::tungstenite::protocol::Message;
use sysinfo::System;

#[derive(Serialize)]
struct CommandResult<'a> {
    r#type: &'a str,
    command_id: &'a str,
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct StatusReport<'a, S: Serialize> {
    r#type: &'a str,
    command_id: &'a str,
    status: S,
}

#[derive(Serialize)]
struct RegistrationRequest {
    available_max_gib: u64,
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

pub struct Network {
    backend_ws_url: String,
    auth_token: String,
    node_id: String,
    command_sender: mpsc::Sender<BackendCommand>,
}

impl Network {
    pub fn init(
        backend_ws_url: String,
        auth_token: String,
        node_id: String,
        command_sender: mpsc::Sender<BackendCommand>,
    ) -> Self {
        Self {
            backend_ws_url,
            auth_token,
            node_id,
            command_sender,
        }
    }

    pub async fn run_connection_loop(&self, mut outgoing_responses_rx: mpsc::Receiver<Message>) -> Result<()> {
        let mut attempts: u64 = 0;
        let base_delay_ms = 1000;
        let max_delay_ms = 60000;

        loop {
            info!("Attempting to connect to WebSocket: {}. Attempt #{}", self.backend_ws_url, attempts + 1);

            match self.establish_and_process_messages(&mut outgoing_responses_rx).await {
                Ok(_) => {
                    info!("WebSocket connection closed gracefully or stream ended. Re-attempting connection.");
                    attempts = 0;
                }
                Err(e) => {
                    error!("WebSocket connection error: {:#}", e);
                    attempts += 1;
                }
            }

            let delay_ms = std::cmp::min(base_delay_ms * 2_u64.pow(attempts.saturating_sub(1) as u32), max_delay_ms);
            info!("Retrying connection in {}ms...", delay_ms);
            sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    async fn establish_and_process_messages(&self, outgoing_responses_rx: &mut mpsc::Receiver<Message>) -> Result<()> {
        let (ws_stream, response) = if self.backend_ws_url.starts_with("wss://") {
            // Create TLS connector that accepts invalid certificates for self-signed certs
            let native_tls_connector = native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()
                .context("Failed to build native TLS connector")?;
            
            let connector = Connector::NativeTls(native_tls_connector);
            
            connect_async_tls_with_config(&self.backend_ws_url, None, false, Some(connector))
                .await
                .with_context(|| format!("Failed to connect to backend WebSocket: {}", self.backend_ws_url))?
        } else {
            connect_async(&self.backend_ws_url)
                .await
                .with_context(|| format!("Failed to connect to backend WebSocket: {}", self.backend_ws_url))?
        };
        debug!("WebSocket handshake response: {:?}", response);
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

        // Timeout Auth if it takes to long
        const AUTH_TIMEOUT_SECONDS: u64 = 10;
        match timeout(Duration::from_secs(AUTH_TIMEOUT_SECONDS), ws_receiver.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(response_val) => {
                        if response_val.get("type").and_then(serde_json::Value::as_str) == Some("AUTH_SUCCESS") {
                            info!("Authentication successful.");
                        } else {
                            return Err(anyhow!("Authentication failed: {:?}", response_val));
                        }
                    }
                    Err(e) => return Err(anyhow!(e)).context("Failed to parse auth response JSON"),
                }
            }
            Ok(Some(Ok(other_msg))) => return Err(anyhow!("Unexpected message type during auth: {:?}", other_msg)),
            Ok(Some(Err(e))) => return Err(anyhow::Error::from(e)).context("Error waiting for auth response"),
            #[allow(non_snake_case)]
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
                                // Process command recieved
                                Message::Text(command) => {
                                    debug!("Received Command: {}", command);
                                    if let Err(e) = self.process_message(command).await {
                                        error!("Error processing incoming message: {}", e);
                                    }
                                }

                                // Other possible responses, not usual for use
                                Message::Binary(_) => warn!("Received binary message, which is not currently processed."),
                                Message::Ping(ping_data) => {
                                    debug!("Received Ping, sending Pong.");
                                    if let Err(e) = ws_sender.send(Message::Pong(ping_data)).await {
                                        error!("Failed to send Pong: {}", e);
                                        return Err(e.into());
                                    }
                                }
                                Message::Pong(_) => debug!("Received Pong from server."),
                                Message::Close(close_frame) => {
                                    info!("WebSocket connection closed by peer: {:?}", close_frame);
                                    return Ok(());
                                }
                                Message::Frame(_) => {}
                            }
                        }
                        Err(e) => {
                            error!("WebSocket receive error: {}", e);
                            return Err(e.into());
                        }
                    }
                }

                // Reply with command status
                Some(response_to_send) = outgoing_responses_rx.recv() => {
                    debug!("Sending message: {:?}", response_to_send);
                    if let Err(e) = ws_sender.send(response_to_send).await {
                        error!("Failed to send outgoing response via WebSocket: {}", e);
                        return Err(e.into());
                    }
                }
                else => {
                    info!("WebSocket receiver stream ended or outgoing_responses_rx closed. Terminating current connection processing.");
                    return Ok(());
                }
            }
        }
    }

    async fn process_message(&self, message_text: String) -> Result<()> {
        match serde_json::from_str::<BackendCommand>(&message_text) {
            Ok(command) => {
                debug!("Parsed command: {:?}", command);
                if let Err(e) = self.command_sender.send(command).await {
                    error!("Failed to send parsed command to internal channel: {}. Channel might be closed.", e);
                    return Err(e.into());
                }
            }
            Err(e) => {
                error!("Failed to parse incoming message as BackendCommand: {}. Message: '{}'", e, message_text);
                if let Err(send_err) = self.command_sender.send(BackendCommand::Unknown).await {
                    error!("Failed to send Unknown command to internal channel: {}", send_err);
                }
            }
        }
        Ok(())
    }
}

// Public functions to be called by commands::handle_command to send responses
pub async fn send_command_result(response_tx: &mpsc::Sender<Message>, command_id: String, operation_result: Result<()>) -> Result<()> {
    let response = CommandResult {
        r#type: "COMMAND_RESULT",
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

pub async fn send_status_report<S: Serialize>(response_tx: &mpsc::Sender<Message>, command_id: String, status_data: S) -> Result<()> {
    let response = StatusReport {
        r#type: "STATUS_REPORT",
        command_id: &command_id,
        status: status_data,
    };

    let response_text = serde_json::to_string(&response)
        .context("Failed to serialize status report")?;

    response_tx.send(Message::Text(response_text)).await
        .context("Failed to send status report")?;
    Ok(())
}

async fn register_node(config: &mut Config, http_client: &HttpClient) -> Result<()> {
    info!("Node ID or Auth Token is missing. Starting registration process.");

    let mut sys = System::new_all();
    sys.refresh_all(); // Make sure data is up-to-date

    // Get system info for storage location information
    let system_info = SystemInfo {
        hostname: System::host_name().unwrap_or_else(|| "unknown_hostname".to_string()),
        os_version: System::long_os_version().unwrap_or_else(|| "unknown_os_version".to_string()),
    };

    // Create payload
    let registration_payload = RegistrationRequest {
        available_max_gib: config.max_storage_gib,
        system_info,
    };

    let backend_api_url = config.backend_api_url.clone();

    // Generate api endpoint
    let registration_url = format!("{}/api/register-node", backend_api_url);
    info!("Registering node with backend: {}", registration_url);

    // Send the payload and wait for response
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
        // Update the config's backend_api_url if it was changed
        config.backend_api_url = backend_api_url;
        save_config(config).await.context("Failed to save updated config after registration")?;
        info!("Configuration updated with new Node ID, Auth Token, and potentially upgraded API URL.");
        Ok(())
    } else {
        let error_body = response.text().await.unwrap_or_else(|_| "No error body".to_string());
        error!("Registration failed. Status: {}. Body: {}", status, error_body);
        Err(anyhow!("Registration failed with status: {}", status))
    }
}


pub async fn start_communication_loop(mut config: Config, used_space_bytes: Arc<AtomicU64>, max_storage_bytes: Arc<AtomicU64>, chunk_count: Arc<AtomicU64>) -> Result<()> {
    let http_client = HttpClient::builder()
        .danger_accept_invalid_certs(true) // Allow invalid certs for registration endpoint
        .build()?;

    // Ensure backend_ws_url has a scheme, but allow WS for local development
    if !config.backend_ws_url.starts_with("ws://") && !config.backend_ws_url.starts_with("wss://") {
        warn!("Configured backend_ws_url does not specify a scheme. Assuming WS for local development: {}", config.backend_ws_url);
        config.backend_ws_url = format!("ws://{}", config.backend_ws_url);
    }

    // Ensure backend_api_url has a scheme, but allow HTTP for local development
    if !config.backend_api_url.starts_with("http://") && !config.backend_api_url.starts_with("https://") {
        warn!("Configured backend_api_url does not specify a scheme. Assuming HTTP for local development: {}", config.backend_api_url);
        config.backend_api_url = format!("http://{}", config.backend_api_url);
    }


    // Register if needed (may mutate config)
    if config.node_id.is_empty() || config.auth_token.is_empty() {
        register_node(&mut config, &http_client)
            .await
            .context("Node registration process failed")?;
    } else {
        info!("Node ID and Auth Token found in config.");
    }

    // Create channels for communication
    let (backend_command_tx, mut backend_command_rx) = mpsc::channel::<BackendCommand>(32);
    let (outgoing_responses_tx, outgoing_responses_rx) = mpsc::channel::<Message>(32);

    // Initialize Network struct with potentially updated config
    let network = Network::init(
        config.backend_ws_url.clone(),
        config.auth_token.clone(),
        config.node_id.clone(),
        backend_command_tx,
    );

    // Clone necessary items for the command handler task
    let storage_path_clone: PathBuf = config.storage_path.clone();
    let responses_tx_clone = outgoing_responses_tx.clone();
    let handler_used_space = used_space_bytes.clone();
    let handler_max_space = max_storage_bytes.clone();
    let handler_chunk_count = chunk_count.clone();

    // Spawn the command handler task
    tokio::spawn(async move {
        info!("Command handler task started.");
        while let Some(command) = backend_command_rx.recv().await {
            debug!("Command handler received: {:?}", command);
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

    // Run the main network connection loop
    network.run_connection_loop(outgoing_responses_rx).await
}