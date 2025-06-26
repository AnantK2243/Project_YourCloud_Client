// src/network.rs

use crate::commands::BackendCommand;
use crate::config::Config;
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{connect_async_tls_with_config, connect_async_with_config, Connector};

#[derive(Serialize)]
struct CommandResult<'a> {
    r#type: &'a str,
    command_id: &'a str,
    chunk_id: &'a str,
    success: bool,
    storage_delta: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct StatusReport<'a, S: Serialize> {
    r#type: &'a str,
    command_id: &'a str,
    status: S,
}

pub struct Network {
    ws_url: String,
    auth_token: String,
    node_id: String,
    command_sender: mpsc::Sender<BackendCommand>,
}

impl Network {
    pub fn init(
        ws_url: String,
        auth_token: String,
        node_id: String,
        command_sender: mpsc::Sender<BackendCommand>,
    ) -> Self {
        let network = Self {
            ws_url,
            auth_token,
            node_id,
            command_sender,
        };
        network
    }

    pub async fn run_connection_loop(
        &self,
        mut outgoing_responses_rx: mpsc::Receiver<Message>,
    ) -> Result<()> {
        let mut attempts: u64 = 0;
        let base_delay_ms = 1000;
        let max_delay_ms = 60000;

        loop {
            info!(
                "Attempting to connect to WebSocket: {}. Attempt #{}",
                self.ws_url,
                attempts + 1
            );

            match self
                .establish_and_process_messages(&mut outgoing_responses_rx)
                .await
            {
                Ok(_) => {
                    info!("WebSocket connection closed gracefully or stream ended. Re-attempting connection.");
                    attempts = 0;
                }
                Err(e) => {
                    error!("WebSocket connection error: {:#}", e);
                    attempts += 1;
                }
            }

            let delay_ms = std::cmp::min(
                base_delay_ms * 2_u64.pow(attempts.saturating_sub(1) as u32),
                max_delay_ms,
            );
            info!("Retrying connection in {}ms...", delay_ms);
            sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    async fn establish_and_process_messages(
        &self,
        outgoing_responses_rx: &mut mpsc::Receiver<Message>,
    ) -> Result<()> {
        // Configure WebSocket
        let ws_config = WebSocketConfig {
            max_message_size: Some(64 * 1024 * 1024), // 64MB
            max_frame_size: Some(64 * 1024 * 1024),
            ..Default::default()
        };

        let (ws_stream, response) = if self.ws_url.starts_with("wss://") {
            // Create TLS connector that accepts invalid certificates for self-signed certs
            let native_tls_connector = native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()
                .context("Failed to build native TLS connector")?;

            let connector = Connector::NativeTls(native_tls_connector);

            connect_async_tls_with_config(&self.ws_url, Some(ws_config), false, Some(connector))
                .await
                .with_context(|| {
                    format!("Failed to connect to backend WebSocket: {}", self.ws_url)
                })?
        } else {
            connect_async_with_config(&self.ws_url, Some(ws_config), false)
                .await
                .with_context(|| {
                    format!("Failed to connect to backend WebSocket: {}", self.ws_url)
                })?
        };
        debug!("WebSocket handshake response: {:?}", response);
        info!(
            "Successfully connected to backend WebSocket: {}",
            self.ws_url
        );

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

        // Timeout Auth if it takes too long
        const AUTH_TIMEOUT_SECONDS: u64 = 10;
        match timeout(
            Duration::from_secs(AUTH_TIMEOUT_SECONDS),
            ws_receiver.next(),
        )
        .await
        {
            Ok(Some(Ok(Message::Text(text)))) => {
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(response_val) => {
                        match response_val.get("type").and_then(serde_json::Value::as_str) {
                            Some("AUTH_SUCCESS") => {
                                info!("Authentication successful.");
                            }
                            Some("AUTH_FAILED") => {
                                let reason = response_val
                                    .get("reason")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or("Unknown reason");
                                return Err(anyhow!("Authentication failed: {}", reason));
                            }
                            Some(other_type) => {
                                return Err(anyhow!(
                                    "Unexpected auth response type: {}",
                                    other_type
                                ));
                            }
                            None => {
                                return Err(anyhow!(
                                    "Authentication response missing type field: {:?}",
                                    response_val
                                ));
                            }
                        }
                    }
                    Err(e) => return Err(anyhow!(e)).context("Failed to parse auth response JSON"),
                }
            }
            Ok(Some(Ok(Message::Close(close_frame)))) => {
                return Err(anyhow!(
                    "Connection closed by server during auth: {:?}",
                    close_frame
                ))
            }
            Ok(Some(Ok(other_msg))) => {
                return Err(anyhow!(
                    "Unexpected message type during auth: {:?}",
                    other_msg
                ))
            }
            Ok(Some(Err(e))) => {
                return Err(anyhow::Error::from(e)).context("WebSocket error during authentication")
            }
            Ok(None) => return Err(anyhow!("Connection closed by peer before auth response")),
            Err(_) => {
                return Err(anyhow!(
                    "Timed out waiting for auth response after {} seconds",
                    AUTH_TIMEOUT_SECONDS
                ))
            }
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
                                    if let Err(e) = self.process_message(command).await {
                                        error!("Error processing incoming message: {}", e);
                                    }
                                }

                                // Handle combined binary messages
                                Message::Binary(data) => {
                                    if let Err(e) = self.process_binary_message(data).await {
                                        error!("Error processing combined binary message: {}", e);
                                    }
                                }
                                // Other message types
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
                // Use try_send to avoid blocking, but handle channel full scenario
                match self.command_sender.try_send(command.clone()) {
                    Ok(_) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!("Command channel is full, applying backpressure");
                        // Try with timeout as fallback
                        match timeout(Duration::from_secs(5), self.command_sender.send(command))
                            .await
                        {
                            Ok(Ok(_)) => {}
                            Ok(Err(e)) => {
                                error!("Failed to send command after backpressure: {}", e);
                                return Err(e.into());
                            }
                            Err(_) => {
                                error!("Command channel send timed out after backpressure");
                                return Err(anyhow!("Command processing too slow, channel full"));
                            }
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        error!("Command channel is closed");
                        return Err(anyhow!("Command channel closed"));
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to parse incoming message as BackendCommand: {}. Message: '{}'",
                    e, message_text
                );
                if let Err(send_err) = self.command_sender.try_send(BackendCommand::Unknown) {
                    match send_err {
                        mpsc::error::TrySendError::Full(_) => {
                            warn!("Cannot send Unknown command, channel full");
                        }
                        mpsc::error::TrySendError::Closed(_) => {
                            error!("Cannot send Unknown command, channel closed");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_binary_message(&self, binary_data: Vec<u8>) -> Result<()> {
        if binary_data.len() < 4 {
            return Err(anyhow!("Binary message too short for header"));
        }

        // Read JSON length (first 4 bytes, little-endian)
        let json_length = u32::from_le_bytes([
            binary_data[0],
            binary_data[1],
            binary_data[2],
            binary_data[3],
        ]) as usize;

        if binary_data.len() < 4 + json_length {
            return Err(anyhow!("Binary message too short for JSON data"));
        }

        // Extract JSON command
        #[derive(Deserialize)]
        struct ParsedJson {
            command_type: String,
            chunk_id: String,
            data_size: u64,
            command_id: String,
        }

        let json_data = &binary_data[4..4 + json_length];
        let command: ParsedJson = serde_json::from_slice(json_data)
            .context("Failed to parse JSON command from binary message")?;

        // Validate command fields
        if command.command_type != "STORE_CHUNK" {
            return Err(anyhow!(
                "Unsupported command type: {}",
                command.command_type
            ));
        }

        // Extract the payload
        let binary_payload = if binary_data.len() > 4 + json_length {
            binary_data[4 + json_length..].to_vec()
        } else {
            Vec::new()
        };

        // Check size of final binary data
        if binary_payload.len() as u64 != command.data_size {
            return Err(anyhow!(
                "Binary data size mismatch: expected {}, got {}",
                command.data_size,
                binary_payload.len() as u64
            ));
        }

        // Create the BackendCommand based on the parsed JSON
        let command = BackendCommand::StoreChunk {
            command_id: command.command_id,
            data_size: command.data_size,
            binary_data: if binary_payload.is_empty() {
                None
            } else {
                Some(binary_payload)
            },
            chunk_id: command.chunk_id,
        };

        // Send to command handler
        if let Err(e) = self.command_sender.send(command).await {
            error!("Failed to send command to handler: {}", e);
            return Err(e.into());
        }

        Ok(())
    }
}

// Public functions to be called by commands::handle_command to send responses
pub async fn send_command_result(
    response_tx: &mpsc::Sender<Message>,
    command_id: String,
    chunk_id: String,
    operation_result: Result<()>,
    storage_delta: Option<i64>,
) -> Result<()> {
    let response = CommandResult {
        r#type: "COMMAND_RESULT",
        command_id: &command_id,
        chunk_id: &chunk_id,
        success: operation_result.is_ok(),
        error: operation_result.err().map(|e| e.to_string()),
        storage_delta,
    };

    let response_text =
        serde_json::to_string(&response).context("Failed to serialize command result")?;

    response_tx
        .send(Message::Text(response_text))
        .await
        .context("Failed to send command result")?;
    Ok(())
}

pub async fn send_status_report<S: Serialize>(
    response_tx: &mpsc::Sender<Message>,
    command_id: String,
    status_data: S,
) -> Result<()> {
    let response = StatusReport {
        r#type: "STATUS_REPORT",
        command_id: &command_id,
        status: status_data,
    };

    let response_text =
        serde_json::to_string(&response).context("Failed to serialize status report")?;

    response_tx
        .send(Message::Text(response_text))
        .await
        .context("Failed to send status report")?;
    Ok(())
}

pub async fn send_chunk_data(
    response_tx: &mpsc::Sender<Message>,
    command_id: String,
    binary_data: Vec<u8>,
) -> Result<()> {
    // Create response header with frame info
    let response_header = serde_json::json!({
        "type": "GET_CHUNK_RESULT",
        "success": true,
        "command_id": command_id,
        "data_size": binary_data.len()
    });
    let header_json = serde_json::to_string(&response_header)
        .context("Failed to serialize chunk response header")?;
    let header_bytes = header_json.as_bytes();

    // Create combined message: [4 bytes: json_length][json_header][binary_data]
    let mut combined_message = Vec::with_capacity(4 + header_bytes.len() + binary_data.len());

    // Write JSON header length
    combined_message.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());

    // Write JSON header
    combined_message.extend_from_slice(header_bytes);

    // Write binary data
    combined_message.extend_from_slice(&binary_data);

    // Send as binary WebSocket message
    response_tx
        .send(Message::Binary(combined_message))
        .await
        .context("Failed to send binary chunk frame")?;

    Ok(())
}

pub async fn start_communication_loop(
    config: Config,
    ws_url: String,
    used_space_bytes: Arc<AtomicU64>,
    max_storage_bytes: Arc<AtomicU64>,
    chunk_count: Arc<AtomicU64>,
) -> Result<()> {
    // Ensure backend_ws_url has a scheme, but allow WS for local development
    let ws_url = if !ws_url.starts_with("ws://") && !ws_url.starts_with("wss://") {
        warn!("Configured backend_ws_url does not specify a scheme. Assuming WS for local development: {}", ws_url);
        format!("ws://{}", ws_url)
    } else {
        ws_url
    };

    // Check if node is properly configured
    if config.node_id.is_empty() || config.auth_token.is_empty() {
        return Err(anyhow!("Node not registered"));
    }

    // Create channels for communication
    let (backend_command_tx, mut backend_command_rx) = mpsc::channel::<BackendCommand>(256);
    let (outgoing_responses_tx, outgoing_responses_rx) = mpsc::channel::<Message>(256);

    // Initialize Network struct with updated config
    let network = Network::init(
        ws_url.clone(),
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
