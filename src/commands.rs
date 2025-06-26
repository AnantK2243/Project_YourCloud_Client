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
#[serde(tag = "command_type", rename_all = "SCREAMING_SNAKE_CASE")] // Use tag for easy parsing
pub enum BackendCommand {
    StoreChunk {
        command_id: String,
        data_size: u64,
        binary_data: Option<Vec<u8>>,
        chunk_id: String,
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
        // Store chunk data
        BackendCommand::StoreChunk {
            command_id,
            data_size,
            binary_data,
            chunk_id,
        } => {
            // Validate chunk_id
            if uuid::Uuid::parse_str(&chunk_id).is_err() {
                send_result(
                    &response_tx,
                    command_id,
                    chunk_id,
                    Err(anyhow!("Invalid chunk_id: not a valid UUID")),
                    None,
                ).await?;
                return Ok(());
            }

            // Check if chunk_id already exists
            if storage::check_chunk_exists(&storage_path, &chunk_id).await? {
                send_result(
                    &response_tx,
                    command_id,
                    chunk_id.clone(),
                    Err(anyhow!("Chunk ID collision: {} already exists", chunk_id)),
                    None,
                )
                .await?;
                return Ok(());
            }

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
                                    chunk_id.clone(),
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

            // Store failed - send error response
            send_result(
                &response_tx,
                command_id,
                chunk_id,
                result,
                None
            ).await?;
        }

        // Get chunk and return binary data
        BackendCommand::GetChunk {
            command_id,
            chunk_id,
        } => {
            // Validate chunk_id
            if uuid::Uuid::parse_str(&chunk_id).is_err() {
                send_result(
                    &response_tx,
                    command_id,
                    chunk_id,
                    Err(anyhow!("Invalid chunk_id: not a valid UUID")),
                    None,
                ).await?;
                return Ok(());
            }

            let result = storage::retrieve_chunk_data_from_disk(&storage_path, &chunk_id).await;

            match result {
                Ok(chunk_data) => {
                    network::send_chunk_data(&response_tx, command_id, chunk_data).await?;
                }
                Err(e) => {
                    send_result(
                        &response_tx,
                        command_id,
                        chunk_id,
                        Err(e),
                        None
                    ).await?;
                }
            }
        }

        // Remove file from store
        BackendCommand::DeleteChunk {
            command_id,
            chunk_id,
        } => {
            // Validate chunk_id
            if uuid::Uuid::parse_str(&chunk_id).is_err() {
                send_result(
                    &response_tx,
                    command_id,
                    chunk_id,
                    Err(anyhow!("Invalid chunk_id: not a valid UUID")),
                    None,
                ).await?;
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
                        chunk_id,
                        Ok(()),
                        Some(-(deleted_size as i64)),
                    )
                    .await?;
                }
                Ok(None) => {
                    // Chunk didn't exist, no storage change
                    send_result(
                        &response_tx,
                        command_id,
                        chunk_id,
                        Ok(()),
                        None
                    ).await?;
                }
                Err(e) => {
                    send_result(
                        &response_tx,
                        command_id,
                        chunk_id,
                        Err(e),
                        None
                    ).await?;
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

async fn send_result(
    response_tx: &mpsc::Sender<Message>,
    command_id: String,
    chunk_id: String,
    result: Result<()>,
    storage_delta: Option<i64>,
) -> Result<()> {
    if let Err(e) = &result {
        error!("Operation for command {} failed: {}", command_id, e);
    }
    network::send_command_result(response_tx, command_id, chunk_id, result, storage_delta).await
}
