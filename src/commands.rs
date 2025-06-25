// src/commands.rs

use crate::network;
use crate::storage;
use anyhow::{anyhow, Result};
use log::{error, warn};
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeStatus {
    pub used_space_bytes: u64,
    pub max_space_bytes: u64,
    pub current_chunk_count: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "command_type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BackendCommand {
    StoreChunk {
        command_id: String,
        chunk_id: String,
        data_size: u64,
        binary_data: Option<Vec<u8>>,
    },
    GetChunk {
        command_id: String,
        chunk_id: String,
    },
    DeleteChunk {
        command_id: String,
        chunk_id: String,
    },
    CheckChunk {
        command_id: String,
        chunk_id: String,
    },
    StatusRequest {
        command_id: String,
    },
    #[serde(other)] // Error Out Unrecognized commands
    Unknown,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WebRTCCommand {
    P2pForward {
        #[serde(rename = "sessionId")]
        session_id: String,
        #[serde(rename = "fromUserId")]
        from_user_id: String,
        #[serde(rename = "fileMetadata")]
        file_metadata: serde_json::Value,
    },
    P2pRelay {
        #[serde(rename = "sessionId")]
        session_id: String,
        payload: serde_json::Value,
    },
    P2pClose {
        #[serde(rename = "sessionId")]
        session_id: String,
        reason: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub enum Command {
    Backend(BackendCommand),
    WebRTC(WebRTCCommand),
}

pub async fn handle_command(
    command: Command,
    storage_path: std::path::PathBuf,
    response_tx: mpsc::Sender<Message>,
    current_used_space_bytes: Arc<AtomicU64>,
    max_storage_bytes: Arc<AtomicU64>,
    current_chunk_count: Arc<AtomicU64>,
) -> Result<()> {
    match command {
        Command::Backend(backend_cmd) => {
            handle_backend_command(
                backend_cmd,
                storage_path,
                response_tx,
                current_used_space_bytes,
                max_storage_bytes,
                current_chunk_count,
            )
            .await
        }
        Command::WebRTC(webrtc_cmd) => {
            handle_webrtc_command(
                webrtc_cmd,
                storage_path,
                response_tx,
                current_used_space_bytes,
                max_storage_bytes,
                current_chunk_count,
            )
            .await
        }
    }
}

async fn handle_backend_command(
    command: BackendCommand,
    storage_path: std::path::PathBuf,
    response_tx: mpsc::Sender<Message>,
    current_used_space_bytes: Arc<AtomicU64>,
    max_storage_bytes: Arc<AtomicU64>,
    current_chunk_count: Arc<AtomicU64>,
) -> Result<()> {
    match command {
        // Store chunk data
        BackendCommand::StoreChunk {
            command_id,
            chunk_id,
            data_size,
            binary_data,
        } => {
            let result = match binary_data {
                Some(chunk_data) => {
                    // Verify data size matches expected
                    if chunk_data.len() != data_size as usize {
                        Err(anyhow!(
                            "Binary data size mismatch: expected {}, got {}",
                            data_size,
                            chunk_data.len()
                        ))
                    } else {
                        match storage::store_chunk_data_to_disk(
                            &storage_path,
                            &chunk_id,
                            &chunk_data,
                            &current_used_space_bytes,
                            &max_storage_bytes,
                            &current_chunk_count,
                        )
                        .await
                        {
                            Ok(chunk_size) => {
                                // Send positive storage delta for stored chunk
                                send_result(
                                    &response_tx,
                                    command_id,
                                    Ok(()),
                                    Some(chunk_size as i64),
                                )
                                .await?;
                                return Ok(());
                            }
                            Err(e) => Err(e),
                        }
                    }
                }
                None => Err(anyhow!("No binary data provided for STORE_CHUNK command")),
            };

            send_result(&response_tx, command_id, result, None).await?;
        }

        // Get chunk and return binary data
        BackendCommand::GetChunk {
            command_id,
            chunk_id,
        } => {
            // Validate chunk_id
            if chunk_id.is_empty() || chunk_id.len() > 255 {
                send_result(
                    &response_tx,
                    command_id,
                    Err(anyhow!("Invalid chunk_id: empty or too long")),
                    None,
                )
                .await?;
                return Ok(());
            }

            if chunk_id.contains("..") || chunk_id.contains("/") || chunk_id.contains("\\") {
                send_result(
                    &response_tx,
                    command_id,
                    Err(anyhow!("Invalid chunk_id: contains unsafe characters")),
                    None,
                )
                .await?;
                return Ok(());
            }

            let result = storage::retrieve_chunk_data_from_disk(&storage_path, &chunk_id).await;

            match result {
                Ok(chunk_data) => {
                    network::send_chunk_data(&response_tx, command_id, chunk_data).await?;
                }
                Err(e) => {
                    send_result(&response_tx, command_id, Err(e), None).await?;
                }
            }
        }

        // Remove file from store
        BackendCommand::DeleteChunk {
            command_id,
            chunk_id,
        } => {
            // Validate chunk_id
            if chunk_id.is_empty() || chunk_id.len() > 255 {
                send_result(
                    &response_tx,
                    command_id,
                    Err(anyhow!("Invalid chunk_id: empty or too long")),
                    None,
                )
                .await?;
                return Ok(());
            }

            if chunk_id.contains("..") || chunk_id.contains("/") || chunk_id.contains("\\") {
                send_result(
                    &response_tx,
                    command_id,
                    Err(anyhow!("Invalid chunk_id: contains unsafe characters")),
                    None,
                )
                .await?;
                return Ok(());
            }

            match storage::delete_chunk_from_disk(
                &storage_path,
                &chunk_id,
                &current_used_space_bytes,
                &current_chunk_count,
            )
            .await
            {
                Ok(Some(deleted_size)) => {
                    // Send negative storage delta for deleted chunk
                    send_result(
                        &response_tx,
                        command_id,
                        Ok(()),
                        Some(-(deleted_size as i64)),
                    )
                    .await?;
                }
                Ok(None) => {
                    // Chunk didn't exist, no storage change
                    send_result(&response_tx, command_id, Ok(()), None).await?;
                }
                Err(e) => {
                    send_result(&response_tx, command_id, Err(e), None).await?;
                }
            }
        }

        // Check if chunk exists
        BackendCommand::CheckChunk {
            command_id,
            chunk_id,
        } => {
            // Validate chunk_id
            if chunk_id.is_empty() || chunk_id.len() > 255 {
                send_result(
                    &response_tx,
                    command_id,
                    Err(anyhow!("Invalid chunk_id: empty or too long")),
                    None,
                )
                .await?;
                return Ok(());
            }

            if chunk_id.contains("..") || chunk_id.contains("/") || chunk_id.contains("\\") {
                send_result(
                    &response_tx,
                    command_id,
                    Err(anyhow!("Invalid chunk_id: contains unsafe characters")),
                    None,
                )
                .await?;
                return Ok(());
            }

            let exists = storage::check_chunk_exists(&storage_path, &chunk_id).await;
            match exists {
                Ok(found) => {
                    network::send_check_response(&response_tx, command_id, found).await?;
                }
                Err(e) => {
                    send_result(&response_tx, command_id, Err(e), None).await?;
                }
            }
        }

        // Handle status request from server
        BackendCommand::StatusRequest { command_id } => {
            let status = NodeStatus {
                used_space_bytes: current_used_space_bytes.load(Ordering::Relaxed),
                max_space_bytes: max_storage_bytes.load(Ordering::Relaxed),
                current_chunk_count: current_chunk_count.load(Ordering::Relaxed),
            };

            network::send_status_report(&response_tx, command_id, status).await?;
        }

        // Unknown command
        BackendCommand::Unknown => {
            warn!("Received unknown or unparsable command - ignoring.");
        }
    }

    Ok(())
}

async fn handle_webrtc_command(
    command: WebRTCCommand,
    storage_path: std::path::PathBuf,
    response_tx: mpsc::Sender<Message>,
    current_used_space_bytes: Arc<AtomicU64>,
    max_storage_bytes: Arc<AtomicU64>,
    current_chunk_count: Arc<AtomicU64>,
) -> Result<()> {
    match command {
        WebRTCCommand::P2pForward {
            session_id,
            from_user_id,
            file_metadata,
        } => {
            info!(
                "Handling P2P_FORWARD for session {}: from user {}",
                session_id, from_user_id
            );
            info!("File metadata: {:?}", file_metadata);

            // Create actual WebRTC session
            crate::webrtc::create_webrtc_session(
                session_id.clone(),
                from_user_id,
                file_metadata,
                storage_path,
                current_used_space_bytes,
                max_storage_bytes,
                current_chunk_count,
                response_tx,
            )
            .await?;

            info!("WebRTC session created for: {}", session_id);
            Ok(())
        }

        WebRTCCommand::P2pRelay {
            session_id,
            payload,
        } => {
            info!("Handling P2P_RELAY for session {}", session_id);

            if let Some(payload_type) = payload.get("type").and_then(|v| v.as_str()) {
                match payload_type {
                    "offer" => {
                        info!("Processing WebRTC offer for session {}", session_id);
                        if let Some(sdp) = payload.get("sdp").and_then(|v| v.as_str()) {
                            crate::webrtc::process_webrtc_offer(&session_id, sdp, &response_tx)
                                .await?;
                        } else {
                            warn!("Missing SDP in offer for session {}. Payload: {:?}", session_id, payload);
                        }
                    }
                    "ice-candidate" => {
                        info!("Processing ICE candidate for session {}", session_id);
                        if let Some(candidate) = payload.get("candidate") {
                            crate::webrtc::add_webrtc_ice_candidate(&session_id, candidate).await?;
                        } else {
                            warn!("Missing candidate data for session {}", session_id);
                        }
                    }
                    _ => {
                        warn!("Unknown P2P relay payload type: {}", payload_type);
                    }
                }
            }

            Ok(())
        }

        WebRTCCommand::P2pClose { session_id, reason } => {
            info!(
                "Handling P2P_CLOSE for session {}: {:?}",
                session_id, reason
            );
            if let Some(_session) = crate::webrtc::close_webrtc_session(&session_id).await {
                info!("Successfully closed WebRTC session: {}", session_id);
            } else {
                warn!("WebRTC session not found for cleanup: {}", session_id);
            }
            Ok(())
        }
    }
}

async fn send_result(
    response_tx: &mpsc::Sender<Message>,
    command_id: String,
    result: Result<()>,
    storage_delta: Option<i64>,
) -> Result<()> {
    if let Err(e) = &result {
        error!("Operation for command {} failed: {}", command_id, e);
    }
    network::send_command_result(response_tx, command_id, result, storage_delta).await
}
