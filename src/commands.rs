// src/commands.rs

use crate::network;
use crate::storage;
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine as _};
use log::{error, info, warn};
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
#[serde(tag = "command_type", rename_all = "SCREAMING_SNAKE_CASE")] // Use tag for easy parsing
pub enum BackendCommand {
    StoreChunk {
        command_id: String,
        chunk_id: String,
        data: String,
    },
    GetChunk {
        command_id: String,
        chunk_id: String,
    },
    DeleteChunk {
        command_id: String,
        chunk_id: String,
    },
    StatusRequest {
        command_id: String,
    },
    #[serde(other)] // Error Out Unrecognized commands
    Unknown,
}

pub async fn handle_command(
    command: BackendCommand,
    storage_path: std::path::PathBuf,
    response_tx: mpsc::Sender<Message>,
    current_used_space_bytes: Arc<AtomicU64>,
    max_storage_bytes: Arc<AtomicU64>,
    current_chunk_count: Arc<AtomicU64>,
) -> Result<()> {
    match command {
        // Store chunk data directly
        BackendCommand::StoreChunk {
            command_id,
            chunk_id,
            data,
        } => {
            info!("Handling StoreChunk: {}", chunk_id);

            // Decode base64 data
            let result = match general_purpose::STANDARD.decode(&data) {
                Ok(chunk_data) => {
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
                            send_result(&response_tx, command_id, Ok(()), Some(chunk_size as i64))
                                .await?;
                        }
                        Err(e) => {
                            send_result(&response_tx, command_id, Err(e), None).await?;
                        }
                    }
                    return Ok(());
                }
                Err(e) => Err(anyhow!("Failed to decode base64 data: {}", e)),
            };

            send_result(&response_tx, command_id, result, None).await?;
        }

        // Get chunk and return data directly
        BackendCommand::GetChunk {
            command_id,
            chunk_id,
        } => {
            info!("Handling GetChunk: {}", chunk_id);

            let result = storage::retrieve_chunk_data_from_disk(&storage_path, &chunk_id).await;

            match result {
                Ok(chunk_data) => {
                    // Encode data as base64 and send back
                    let encoded_data = general_purpose::STANDARD.encode(&chunk_data);
                    network::send_chunk_data(&response_tx, command_id, encoded_data).await?;
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
            info!("Handling DeleteChunk: {}", chunk_id);

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

        // Handle status request from server
        BackendCommand::StatusRequest { command_id } => {
            info!("Handling StatusRequest");

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
