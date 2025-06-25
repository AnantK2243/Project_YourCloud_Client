// src/network.rs

use crate::commands::{self, BackendCommand, WebRTCConnections};
use crate::config::Config;
use crate::storage::StorageState;
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use serde::Serialize;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{connect_async_tls_with_config, connect_async_with_config, Connector};
use reqwest;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::ice_transport::ice_server::RTCIceServer;

/// Represents a status report sent back to the proxy server.
#[derive(Serialize)]
struct StatusReport<'a, S: Serialize> {
    r#type: &'a str,
    command_id: &'a str,
    status: S,
}

/// Manages the WebSocket connection to the backend proxy server.
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
        Self {
            ws_url,
            auth_token,
            node_id,
            command_sender,
        }
    }

    /// Runs a loop that continuously tries to connect and maintain a WebSocket connection.
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
                    info!("WebSocket connection closed gracefully. Re-attempting connection.");
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

    /// Establishes a single WebSocket connection and handles the message processing loop.
    async fn establish_and_process_messages(
        &self,
        outgoing_responses_rx: &mut mpsc::Receiver<Message>,
    ) -> Result<()> {
        let ws_config = WebSocketConfig {
            max_message_size: Some(64 * 1024 * 1024), // 64MB
            ..Default::default()
        };

        let (ws_stream, response) = if self.ws_url.starts_with("wss://") {
            let native_tls_connector = native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true) // For self-signed certs in dev
                .build()
                .context("Failed to build native TLS connector")?;
            let connector = Connector::NativeTls(native_tls_connector);
            connect_async_tls_with_config(&self.ws_url, Some(ws_config), false, Some(connector))
                .await
                .with_context(|| format!("Failed to connect to WebSocket: {}", self.ws_url))?
        } else {
            connect_async_with_config(&self.ws_url, Some(ws_config), false)
                .await
                .with_context(|| format!("Failed to connect to WebSocket: {}", self.ws_url))?
        };
        debug!("WebSocket handshake response: {:?}", response);

        info!(
            "Successfully connected to backend WebSocket: {}",
            self.ws_url
        );

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

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

        // Wait for authentication response
        const AUTH_TIMEOUT_SECONDS: u64 = 10;
        match timeout(
            Duration::from_secs(AUTH_TIMEOUT_SECONDS),
            ws_receiver.next(),
        )
        .await
        {
            Ok(Some(Ok(Message::Text(text)))) => {
                let response_val: serde_json::Value =
                    serde_json::from_str(&text).context("Failed to parse auth response as JSON")?;
                match response_val.get("type").and_then(|v| v.as_str()) {
                    Some("AUTH_SUCCESS") => info!("Authentication successful."),
                    Some("AUTH_FAILED") => {
                        let reason = response_val
                            .get("reason")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown reason");
                        return Err(anyhow!("Authentication failed: {}", reason));
                    }
                    _ => return Err(anyhow!("Unexpected auth response: {}", text)),
                }
            }
            Ok(Some(other)) => return Err(anyhow!("Unexpected message during auth: {:?}", other)),
            Ok(None) => return Err(anyhow!("Connection closed by peer before auth response")),
            Err(_) => return Err(anyhow!("Timed out waiting for auth response")),
        }

        // Message Handling Loop
        loop {
            tokio::select! {
                // Handle incoming messages from the WebSocket server
                Some(message_result) = ws_receiver.next() => {
                    match message_result {
                        Ok(Message::Text(command)) => {
                            if let Err(e) = self.process_message(command).await {
                                error!("Error processing incoming message: {}", e);
                            }
                        }
                        Ok(Message::Ping(ping_data)) => {
                            debug!("Received Ping, sending Pong.");
                            if let Err(e) = ws_sender.send(Message::Pong(ping_data)).await {
                                error!("Failed to send Pong: {}", e);
                                return Err(e.into());
                            }
                        }
                        Ok(Message::Close(close_frame)) => {
                            info!("WebSocket connection closed by peer: {:?}", close_frame);
                            return Ok(());
                        }
                        Ok(msg) => warn!("Received unhandled message type: {:?}", msg),
                        Err(e) => {
                            error!("WebSocket receive error: {}", e);
                            return Err(e.into());
                        }
                    }
                }
                // Handle outgoing messages to the WebSocket server
                Some(response_to_send) = outgoing_responses_rx.recv() => {
                    debug!("Sending message: {:?}", response_to_send);
                    if let Err(e) = ws_sender.send(response_to_send).await {
                        error!("Failed to send outgoing response via WebSocket: {}", e);
                        return Err(e.into());
                    }
                }
                else => {
                    info!("WebSocket receiver stream ended or outgoing_responses_rx closed. Terminating.");
                    return Ok(());
                }
            }
        }
    }

    /// Parses an incoming message and sends it to the command handler task.
    async fn process_message(&self, message_text: String) -> Result<()> {
        match serde_json::from_str::<BackendCommand>(&message_text) {
            Ok(command) => {
                debug!("Parsed command: {:?}", command);
                if self.command_sender.send(command).await.is_err() {
                    return Err(anyhow!("Command handler channel is closed."));
                }
            }
            Err(e) => {
                error!(
                    "Failed to parse incoming message as BackendCommand: {}. Message: '{}'",
                    e, message_text
                );
                if self
                    .command_sender
                    .send(BackendCommand::Unknown)
                    .await
                    .is_err()
                {
                    error!("Cannot send Unknown command, channel closed");
                }
            }
        }
        Ok(())
    }
}

/// Sends a status report back to the proxy server.
pub async fn send_status_report(
    response_tx: &mpsc::Sender<Message>,
    command_id: String,
    storage_state: Arc<StorageState>,
) -> Result<()> {
    let current_used_space = storage_state
        .current_used_space_bytes
        .load(std::sync::atomic::Ordering::Acquire);
    let current_chunk_count = storage_state
        .current_chunk_count
        .load(std::sync::atomic::Ordering::Acquire);
    let max_space_bytes = storage_state
        .max_storage_bytes
        .load(std::sync::atomic::Ordering::Acquire);

    let response = StatusReport {
        r#type: "STATUS_REPORT",
        command_id: &command_id,
        status: serde_json::json!({
            "used_space_bytes": current_used_space,
            "max_space_bytes": max_space_bytes,
            "current_chunk_count": current_chunk_count,
        }),
    };

    let response_text =
        serde_json::to_string(&response).context("Failed to serialize status report")?;

    response_tx
        .send(Message::Text(response_text))
        .await
        .context("Failed to send status report")?;
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct TurnCredentialsResponse {
    pub ice_servers: Vec<IceServer>,
}

#[derive(Debug, serde::Deserialize)]
struct IceServer {
    pub urls: Vec<String>,
    pub username: Option<String>,
    pub credential: Option<String>,
}

async fn get_turn_credentials(api_url: &str, auth_token: &str) -> Result<RTCConfiguration> {
    let client = reqwest::Client::new();
    let response = client
        .post(&format!("{}/turn-credentials", api_url))
        .header("Authorization", format!("Bearer {}", auth_token))
        .send()
        .await
        .context("Failed to send request to /turn-credentials endpoint")?
        .json::<TurnCredentialsResponse>()
        .await
        .context("Failed to parse JSON response from /turn-credentials")?;

    // Manually map the fields from the response struct to the webrtc-rs struct
    let ice_servers = response
        .ice_servers
        .into_iter()
        .map(|s| RTCIceServer {
            urls: s.urls,
            username: s.username.unwrap_or_default(),
            credential: s.credential.unwrap_or_default(),
            ..Default::default()
        })
        .collect();

    Ok(RTCConfiguration {
        ice_servers,
        ..Default::default()
    })
}


/// Initializes and starts the primary communication loop and command handler task.
pub async fn start_communication_loop(
    config: Config,
    ws_url: String,
    storage_state: Arc<StorageState>,
) -> Result<()> {
    // Ensure backend_ws_url has a scheme
    let ws_url = if !ws_url.starts_with("ws://") && !ws_url.starts_with("wss://") {
        warn!(
            "Configured backend_ws_url does not specify a scheme. Assuming wss://: {}",
            ws_url
        );
        format!("wss://{}", ws_url)
    } else {
        ws_url
    };

    if config.node_id.is_empty() || config.auth_token.is_empty() {
        return Err(anyhow!("Node is not configured with an ID and auth token."));
    }

    // Create channels for communication
    let (backend_command_tx, mut backend_command_rx) = mpsc::channel::<BackendCommand>(256);
    let (outgoing_responses_tx, outgoing_responses_rx) = mpsc::channel::<Message>(256);

    // Create the shared state for WebRTC connections
    let webrtc_connections: WebRTCConnections = Arc::new(Mutex::new(HashMap::new()));

    // Clone necessary items for the command handler task.
    let responses_tx_clone = outgoing_responses_tx.clone();
    let webrtc_connections_clone = webrtc_connections.clone();
    let storage_state_clone = storage_state.clone();
    let storage_path_clone = config.storage_path.clone();

    // Fetch TURN credentials before starting the command handler
    let rtc_config = match get_turn_credentials(&config.api_url, &config.auth_token).await {
        Ok(c) => c,
        Err(e) => {
            log::warn!("Failed to get TURN credentials: {}. Falling back to STUN only.", e);
            // Fallback configuration
            RTCConfiguration {
                ice_servers: vec![RTCIceServer {
                    urls: vec!["stun:stun.l.google.com:19302".to_owned()],
                    ..Default::default()
                }],
                ..Default::default()
            }
        }
    };

    // Spawn the command handler task
    tokio::spawn(async move {
        while let Some(command) = backend_command_rx.recv().await {
            debug!("Command handler received: {:?}", command);
            if let Err(e) = commands::handle_command(
                command,
                responses_tx_clone.clone(),
                storage_state_clone.clone(),
                webrtc_connections_clone.clone(),
                storage_path_clone.clone(), // Pass storage path
                rtc_config.clone(),
            )
            .await
            {
                error!("Error handling command: {:?}", e);
            }
        }
        info!("Command handler task finished (channel closed).");
    });

    // Initialize and run the network communication loop
    let network = Network::init(
        ws_url,
        config.auth_token,
        config.node_id,
        backend_command_tx,
    );

    network.run_connection_loop(outgoing_responses_rx).await
}
