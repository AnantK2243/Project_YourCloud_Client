// src/commands.rs

use serde::{Deserialize, Serialize};
use anyhow::{Result, anyhow};
use crate::storage;
use crate::network;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;
use futures_util::stream::TryStreamExt;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BackendCommand {
    StoreChunk { command_id: String, chunk_id: String, expected_size_bytes: u64, data_source_url: String },
    RetrieveChunk { command_id: String, chunk_id: String, upload_destination_url: String },
    DeleteChunk { command_id: String, chunk_id: String },
    QueryStatus { command_id: String },
    Unknown, // For unparseable commands
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
        BackendCommand::StoreChunk { command_id, chunk_id, expected_size_bytes, data_source_url } => {
            log::info!("Storing chunk: {}", chunk_id);
            
            // Download data from source URL
            let client = reqwest::Client::new();
            let download_result = client.get(&data_source_url).send().await;
            
            let result = match download_result {
                Ok(response) if response.status().is_success() => {
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
        
        BackendCommand::RetrieveChunk { command_id, chunk_id, upload_destination_url } => {
            log::info!("Retrieving chunk: {}", chunk_id);
            
            let result = match storage::retrieve_chunk_from_disk(&storage_path, &chunk_id).await {
                Ok(stream) => {
                    // Convert stream to bytes and upload
                    let client = reqwest::Client::new();
                    let bytes_vec: Result<Vec<bytes::Bytes>> = stream.try_collect().await.map_err(|e| anyhow!("Failed to read chunk: {}", e));
                    
                    match bytes_vec {
                        Ok(chunks) => {
                            let data = chunks.concat();
                            client.put(&upload_destination_url)
                                .body(data)
                                .send()
                                .await
                                .map_err(|e| anyhow!("Upload failed: {}", e))
                                .and_then(|response| {
                                    if response.status().is_success() {
                                        Ok(())
                                    } else {
                                        Err(anyhow!("Upload failed with status: {}", response.status()))
                                    }
                                })
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            };
            
            send_result(&response_tx, command_id, result).await?;
        },
        
        BackendCommand::DeleteChunk { command_id, chunk_id } => {
            log::info!("Deleting chunk: {}", chunk_id);
            
            let result = storage::delete_chunk_from_disk(&storage_path, &chunk_id, &current_used_space_bytes, &current_chunk_count)
                .await
                .and_then(|opt_size| {
                    opt_size.ok_or_else(|| anyhow!("Chunk {} not found", chunk_id)).map(|_| ())
                });
            
            send_result(&response_tx, command_id, result).await?;
        },
        
        BackendCommand::QueryStatus { command_id } => {
            log::info!("Querying status");
            
            let status = storage::get_current_node_status(&current_used_space_bytes, &max_storage_bytes, &current_chunk_count);
            network::send_status_report(&response_tx, command_id, status).await?;
        },
        
        BackendCommand::Unknown => {
            log::warn!("Received unknown command");
            // Don't send a response for unknown commands
        },
    }
    
    Ok(())
}

// Helper function to simplify result sending
async fn send_result(response_tx: &mpsc::Sender<Message>, command_id: String, result: Result<()>) -> Result<()> {
    network::send_command_result(response_tx, command_id, result).await
}