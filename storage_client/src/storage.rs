// src/storage.rs

use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio_util::codec::{FramedRead, BytesCodec};
use bytes::Bytes;
use anyhow::{Result, Context, anyhow};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use futures_util::{Stream, StreamExt, TryStreamExt}; // Added TryStreamExt
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct NodeStatus {
    pub used_space_bytes: u64,
    pub max_space_bytes: u64,
    pub free_space_bytes: u64,
    pub chunk_count: u64,
}

// Helper function to get the path for a chunk
fn get_chunk_path(base_path: &Path, chunk_id: &str) -> PathBuf {
    if chunk_id.len() < 2 {
        return base_path.join(chunk_id);
    }
    let subdir = &chunk_id[..2]; // First two characters for subdirectory
    let chunk_filename = chunk_id.to_string();
    base_path.join(subdir).join(chunk_filename)
}

pub async fn initialize_storage_state( storage_path_base: &Path, max_storage_gib: u64) -> Result<(Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>)> {
    let current_used_space = Arc::new(AtomicU64::new(0));
    let current_chunk_count = Arc::new(AtomicU64::new(0));
    let max_bytes = Arc::new(AtomicU64::new(max_storage_gib * 1024 * 1024 * 1024));

    if !storage_path_base.exists() {
        fs::create_dir_all(storage_path_base).await.with_context(|| format!("Failed to create base storage directory: {:?}", storage_path_base))?;
    }

    let mut main_dir_entries = fs::read_dir(storage_path_base).await
        .with_context(|| format!("Failed to read base storage directory: {:?}", storage_path_base))?;

    while let Some(entry) = main_dir_entries.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            // This is a 2-char subdirectory, e.g., "0a", "ff"
            let mut subdir_entries = fs::read_dir(&path).await
                .with_context(|| format!("Failed to read subdirectory: {:?}", path))?;
            while let Some(chunk_entry) = subdir_entries.next_entry().await? {
                let chunk_path = chunk_entry.path();
                if chunk_path.is_file() {
                    match fs::metadata(&chunk_path).await {
                        Ok(metadata) => {
                            current_used_space.fetch_add(metadata.len(), Ordering::SeqCst);
                            current_chunk_count.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            log::warn!("Failed to get metadata for file {:?}: {}", chunk_path, e);
                        }
                    }
                }
            }
        }
    }
    log::info!(
        "Storage initialized: Used: {} bytes, Max: {} bytes, Chunks: {}",
        current_used_space.load(Ordering::SeqCst),
        max_bytes.load(Ordering::SeqCst),
        current_chunk_count.load(Ordering::SeqCst)
    );
    Ok((current_used_space, max_bytes, current_chunk_count))
}

// Function to store a chunk to disk
pub async fn store_chunk_to_disk(storage_path_base: &Path, chunk_id: &str, mut data_stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin, expected_size_bytes: u64, current_used_space_bytes: &Arc<AtomicU64>, max_storage_bytes: &Arc<AtomicU64>, current_chunk_count: &Arc<AtomicU64>) -> Result<u64> {
    let chunk_path = get_chunk_path(storage_path_base, chunk_id);

    if tokio::fs::try_exists(&chunk_path).await.unwrap_or(false) {
        return Err(anyhow!("Chunk {} already exists. Delete first to overwrite.", chunk_id));
    }

    let current_max = max_storage_bytes.load(Ordering::Relaxed);
    let current_used = current_used_space_bytes.load(Ordering::Relaxed);

    if current_used + expected_size_bytes > current_max {
        return Err(anyhow!(
            "Insufficient storage space. Required: {}, Available: {}. Current usage: {}/{}",
            expected_size_bytes,
            current_max.saturating_sub(current_used),
            current_used,
            current_max
        ));
    }

    // Ensure parent directory exists
    if let Some(parent_dir) = chunk_path.parent() {
        if !parent_dir.exists() {
            fs::create_dir_all(parent_dir).await.with_context(|| format!("Failed to create directory for chunk: {:?}", parent_dir))?;
        }
    } else {
        return Err(anyhow!("Invalid chunk path, cannot determine parent directory: {:?}", chunk_path));
    }
    
    let mut file = fs::File::create(&chunk_path).await.with_context(|| format!("Failed to create file for chunk: {:?}", chunk_path))?;
    let mut bytes_written: u64 = 0;

    while let Some(data_result) = data_stream.next().await {
        let data_bytes = data_result.context("Error streaming chunk data from source")?;
        file.write_all(&data_bytes).await.with_context(|| format!("Failed to write data to chunk file: {:?}", chunk_path))?;
        bytes_written += data_bytes.len() as u64;
    }
    file.sync_all().await.with_context(|| format!("Failed to sync chunk file: {:?}", chunk_path))?;

    if bytes_written != expected_size_bytes {
        fs::remove_file(&chunk_path).await.with_context(|| format!("Failed to delete mismatched chunk file: {:?}", chunk_path))?;
        return Err(anyhow!("Size mismatch for chunk {}: Expected {}, wrote {}. Chunk deleted.", chunk_id, expected_size_bytes, bytes_written));
    }

    // Successfully written, update atomics
    current_used_space_bytes.fetch_add(bytes_written, Ordering::SeqCst);
    current_chunk_count.fetch_add(1, Ordering::SeqCst);
    log::info!("Stored chunk {}: {} bytes. New usage: {} bytes, {} chunks.",
        chunk_id,
        bytes_written,
        current_used_space_bytes.load(Ordering::SeqCst),
        current_chunk_count.load(Ordering::SeqCst)
    );

    Ok(bytes_written)
}

// Function to retrieve a chunk from disk
pub async fn retrieve_chunk_from_disk(storage_path_base: &Path, chunk_id: &str) -> Result<impl Stream<Item = Result<Bytes, std::io::Error>>> {
    let chunk_path = get_chunk_path(storage_path_base, chunk_id);
    if !tokio::fs::try_exists(&chunk_path).await.unwrap_or(false) {
        return Err(anyhow!("Chunk {} not found at {:?}", chunk_id, chunk_path));
    }
    let file = fs::File::open(&chunk_path).await.with_context(|| format!("Chunk {} not found or unreadable at {:?}", chunk_id, chunk_path))?;
    let reader = BufReader::new(file);
    let stream = FramedRead::new(reader, BytesCodec::new()).map_ok(|bytes| bytes.freeze());
    Ok(stream)
}

// Function to delete a chunk from disk
pub async fn delete_chunk_from_disk(storage_path_base: &Path, chunk_id: &str, current_used_space_bytes: &Arc<AtomicU64>, current_chunk_count: &Arc<AtomicU64>) -> Result<Option<u64>> {
    let chunk_path = get_chunk_path(storage_path_base, chunk_id);
    match fs::metadata(&chunk_path).await {
        Ok(metadata) => {
            let size = metadata.len();
            fs::remove_file(&chunk_path).await.with_context(|| format!("Failed to delete chunk file: {:?}", chunk_path))?;
            
            current_used_space_bytes.fetch_sub(size, Ordering::SeqCst);
            current_chunk_count.fetch_sub(1, Ordering::SeqCst);
            log::info!("Deleted chunk {}: {} bytes. New usage: {} bytes, {} chunks.",
                chunk_id,
                size,
                current_used_space_bytes.load(Ordering::SeqCst),
                current_chunk_count.load(Ordering::SeqCst)
            );
            Ok(Some(size))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::warn!("Attempted to delete non-existent chunk: {}", chunk_id);
            Ok(None)
        }
        Err(e) => Err(anyhow!("Error deleting chunk {}: {}", chunk_id, e)),
    }
}

pub fn get_current_node_status(current_used_space_bytes: &Arc<AtomicU64>, max_storage_bytes: &Arc<AtomicU64>, current_chunk_count: &Arc<AtomicU64>) -> NodeStatus {
    let used = current_used_space_bytes.load(Ordering::Relaxed);
    let max_cap = max_storage_bytes.load(Ordering::Relaxed);
    NodeStatus {
        used_space_bytes: used,
        max_space_bytes: max_cap,
        free_space_bytes: max_cap.saturating_sub(used),
        chunk_count: current_chunk_count.load(Ordering::Relaxed),
    }
}