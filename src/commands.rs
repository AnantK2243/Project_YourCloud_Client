// src/commands.rs

use serde::{Deserialize, Serialize};
use anyhow::{Result, anyhow, Context};
use crate::storage;
use crate::network;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;
use reqwest::Client as HttpClient;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "command_type", rename_all = "SCREAMING_SNAKE_CASE")] // Use tag for easy parsing
pub enum BackendCommand {
    StoreChunk { command_id: String, chunk_id: String, expected_size_bytes: u64, data_source_url: String },
    RetrieveChunk { command_id: String, chunk_id: String, upload_destination_url: String },
    DeleteChunk { command_id: String, chunk_id: String },
    QueryStatus { command_id: String },
    #[serde(other)] // Error Out Unrecognized commands
    Unknown,
}

pub async fn handle_command(command: BackendCommand, storage_path: std::path::PathBuf, response_tx: mpsc::Sender<Message>, current_used_space_bytes: Arc<AtomicU64>, max_storage_bytes: Arc<AtomicU64>, current_chunk_count: Arc<AtomicU64>) -> Result<()> {
    let http_client = HttpClient::new();

    match command {
        // Add file to store
        BackendCommand::StoreChunk { command_id, chunk_id, expected_size_bytes, data_source_url } => {
            log::info!("Handling StoreChunk: {}", chunk_id);

            // Get the data from download link
            let download_result = http_client.get(&data_source_url).send().await;

            // Parse result of call
            let result = match download_result {
                Ok(response) if response.status().is_success() => {
                    // Store the chunk to store
                    let data_stream = response.bytes_stream();
                    storage::store_chunk_to_disk(
                        &storage_path,
                        &chunk_id,
                        data_stream,
                        expected_size_bytes,
                        &current_used_space_bytes,
                        &max_storage_bytes,
                        &current_chunk_count
                    ).await.map(|_| ())
                }
                Ok(response) => Err(anyhow!("Download failed with status: {}", response.status())),
                Err(e) => Err(anyhow!("Download request failed: {}", e)),
            };

            send_result(&response_tx, command_id, result).await?;
        },

        // Get file from store
        BackendCommand::RetrieveChunk { command_id, chunk_id, upload_destination_url } => {
            log::info!("Handling RetrieveChunk: {}", chunk_id);

            // Get the required chunk from store
            let result = match storage::retrieve_chunk_from_disk(&storage_path, &chunk_id).await {
                Ok(stream) => {
                    // Upload the file to requested link
                    let body = reqwest::Body::wrap_stream(stream);
                    http_client.put(&upload_destination_url)
                        .body(body)
                        .send()
                        .await
                        .context("Upload request failed")
                        .and_then(|response| {
                            if response.status().is_success() {
                                log::info!("Successfully uploaded chunk {}", chunk_id);
                                Ok(())
                            } else {
                                Err(anyhow!("Upload failed with status: {}", response.status()))
                            }
                        })
                }
                Err(e) => Err(e),
            };

            send_result(&response_tx, command_id, result).await?;
        },

        // Remove file from store
        BackendCommand::DeleteChunk { command_id, chunk_id } => {
            log::info!("Handling DeleteChunk: {}", chunk_id);

            // Delete the chunk_id node from store
            let result = storage::delete_chunk_from_disk(
                &storage_path,
                &chunk_id,
                &current_used_space_bytes,
                &current_chunk_count
            ).await.map(|_| ());

            send_result(&response_tx, command_id, result).await?;
        },

        // Get the status of the file store (space used/available)
        BackendCommand::QueryStatus { command_id } => {
            log::info!("Handling QueryStatus");

            // Get the store status
            let status = storage::get_current_node_status(
                &current_used_space_bytes,
                &max_storage_bytes,
                &current_chunk_count
            );

            // Send the status using network function
            network::send_status_report(&response_tx, command_id, status).await?;
        },

        // Unknown command
        BackendCommand::Unknown => {
            log::warn!("Received unknown or unparseable command - ignoring.");
        },
    }

    Ok(())
}

async fn send_result(response_tx: &mpsc::Sender<Message>, command_id: String, result: Result<()>) -> Result<()> {
    // Helper function to send command results to backend
    if let Err(e) = &result {
        log::error!("Operation for command {} failed: {}", command_id, e);
    }
    network::send_command_result(response_tx, command_id, result).await
}