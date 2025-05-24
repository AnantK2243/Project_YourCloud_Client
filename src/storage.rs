// src/storage.rs

use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufReader}; // Removed AsyncReadExt (using FramedRead)
use tokio_util::codec::{FramedRead, BytesCodec};
use bytes::Bytes;
use anyhow::{Result, Context, anyhow};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use futures_util::{Stream, StreamExt, TryStreamExt};
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct NodeStatus {
    pub used_space_bytes: u64,
    pub max_space_bytes: u64,
    pub free_space_bytes: u64,
    pub chunk_count: u64,
    // Add physical available space? Maybe later via sysinfo call
}

// Helper function to get the path for a chunk
fn get_chunk_path(base_path: &Path, chunk_id: &str) -> PathBuf {
    if chunk_id.len() < 4 { // Use 4 chars (e.g., ab/cd) for better sharding
        return base_path.join(chunk_id); // Fallback for very short IDs (unlikely)
    }
    let subdir1 = &chunk_id[0..2];
    let subdir2 = &chunk_id[2..4];
    let chunk_filename = chunk_id.to_string();
    base_path.join(subdir1).join(subdir2).join(chunk_filename)
}

pub async fn initialize_storage_state(
    storage_path_base: &Path,
    max_storage_gib: u64,
) -> Result<(Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>)> {
    let current_used_space = Arc::new(AtomicU64::new(0));
    let current_chunk_count = Arc::new(AtomicU64::new(0));
    let max_bytes = Arc::new(AtomicU64::new(max_storage_gib * 1024 * 1024 * 1024));

    if !storage_path_base.exists() {
        fs::create_dir_all(storage_path_base)
            .await
            .with_context(|| format!("Failed to create base storage directory: {:?}", storage_path_base))?;
        log::info!("Created base storage directory: {:?}", storage_path_base);
    }

    log::info!("Scanning storage directory {:?} for existing chunks...", storage_path_base);
    let mut total_size = 0u64;
    let mut total_count = 0u64;
    let mut first_level_dirs = fs::read_dir(storage_path_base).await?;

    while let Some(l1_entry) = first_level_dirs.next_entry().await? {
        let l1_path = l1_entry.path();
        if l1_path.is_dir() {
            let mut second_level_dirs = fs::read_dir(&l1_path).await?;
            while let Some(l2_entry) = second_level_dirs.next_entry().await? {
                let l2_path = l2_entry.path();
                if l2_path.is_dir() {
                    let mut chunk_files = fs::read_dir(&l2_path).await?;
                     while let Some(chunk_entry) = chunk_files.next_entry().await? {
                        let chunk_path = chunk_entry.path();
                        if chunk_path.is_file() {
                             match fs::metadata(&chunk_path).await {
                                Ok(metadata) => {
                                    total_size += metadata.len();
                                    total_count += 1;
                                }
                                Err(e) => {
                                    log::warn!("Failed to get metadata for file {:?}: {}", chunk_path, e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    current_used_space.store(total_size, Ordering::SeqCst);
    current_chunk_count.store(total_count, Ordering::SeqCst);

    log::info!(
        "Storage scan complete: Used: {} bytes, Max: {} bytes, Chunks: {}",
        total_size,
        max_bytes.load(Ordering::SeqCst),
        total_count
    );
    Ok((current_used_space, max_bytes, current_chunk_count))
}

// Function to store a chunk to disk
pub async fn store_chunk_to_disk(
    storage_path_base: &Path,
    chunk_id: &str,
    mut data_stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
    expected_size_bytes: u64,
    current_used_space_bytes: &Arc<AtomicU64>,
    max_storage_bytes: &Arc<AtomicU64>,
    current_chunk_count: &Arc<AtomicU64>,
) -> Result<u64> {
    let chunk_path = get_chunk_path(storage_path_base, chunk_id);

    // 1. Check for pre-existence
    if tokio::fs::try_exists(&chunk_path).await.unwrap_or(false) {
        log::warn!("Chunk {} already exists, refusing to overwrite.", chunk_id);
        return Err(anyhow!("Chunk {} already exists.", chunk_id));
    }

    // 2. Check space *before* creating directories/files
    let current_max = max_storage_bytes.load(Ordering::Relaxed);
    let current_used = current_used_space_bytes.load(Ordering::Relaxed);

    if current_used.saturating_add(expected_size_bytes) > current_max {
        return Err(anyhow!(
            "Insufficient storage space. Required: {}, Available: {}. Current: {}/{}",
            expected_size_bytes,
            current_max.saturating_sub(current_used),
            current_used,
            current_max
        ));
    }
    // (Physical space check should ideally happen before calling this or in network.rs)

    // 3. Ensure parent directory exists
    if let Some(parent_dir) = chunk_path.parent() {
        fs::create_dir_all(parent_dir).await.with_context(|| format!("Failed to create directory for chunk: {:?}", parent_dir))?;
    } else {
        return Err(anyhow!("Invalid chunk path, no parent dir: {:?}", chunk_path));
    }

    // 4. Stream data to file
    let mut file = fs::File::create(&chunk_path).await.with_context(|| format!("Failed to create file for chunk: {:?}", chunk_path))?;
    let mut bytes_written: u64 = 0;

    while let Some(data_result) = data_stream.next().await {
        let data_bytes = data_result.context("Error streaming chunk data from source")?;
        file.write_all(&data_bytes).await.with_context(|| format!("Failed to write data to chunk file: {:?}", chunk_path))?;
        bytes_written += data_bytes.len() as u64;
    }
    file.sync_all().await.with_context(|| format!("Failed to sync chunk file: {:?}", chunk_path))?;

    // 5. Verify size and update atomics
    if bytes_written != expected_size_bytes {
        log::error!("Size mismatch for chunk {}: Expected {}, wrote {}. Deleting.", chunk_id, expected_size_bytes, bytes_written);
        fs::remove_file(&chunk_path).await.with_context(|| format!("Failed to delete mismatched chunk file: {:?}", chunk_path))?;
        return Err(anyhow!("Size mismatch. Chunk deleted."));
    }

    current_used_space_bytes.fetch_add(bytes_written, Ordering::SeqCst);
    current_chunk_count.fetch_add(1, Ordering::SeqCst);
    log::info!(
        "Stored chunk {}: {} bytes. New usage: {} bytes, {} chunks.",
        chunk_id,
        bytes_written,
        current_used_space_bytes.load(Ordering::SeqCst),
        current_chunk_count.load(Ordering::SeqCst)
    );

    Ok(bytes_written)
}

// Function to retrieve a chunk from disk as a Stream
pub async fn retrieve_chunk_from_disk(
    storage_path_base: &Path,
    chunk_id: &str,
) -> Result<impl Stream<Item = Result<Bytes, std::io::Error>>> {
    let chunk_path = get_chunk_path(storage_path_base, chunk_id);
    log::debug!("Attempting to retrieve chunk from {:?}", chunk_path);

    if !tokio::fs::try_exists(&chunk_path).await.unwrap_or(false) {
        return Err(anyhow!("Chunk {} not found at {:?}", chunk_id, chunk_path));
    }
    let file = fs::File::open(&chunk_path).await.with_context(|| format!("Failed to open chunk file: {:?}", chunk_path))?;
    let reader = BufReader::new(file);
    let stream = FramedRead::new(reader, BytesCodec::new()).map_ok(|bytes| bytes.freeze());
    Ok(stream)
}

// Function to delete a chunk from disk
pub async fn delete_chunk_from_disk(
    storage_path_base: &Path,
    chunk_id: &str,
    current_used_space_bytes: &Arc<AtomicU64>,
    current_chunk_count: &Arc<AtomicU64>,
) -> Result<Option<u64>> {
    let chunk_path = get_chunk_path(storage_path_base, chunk_id);
    match fs::metadata(&chunk_path).await {
        Ok(metadata) => {
            let size = metadata.len();
            fs::remove_file(&chunk_path).await.with_context(|| format!("Failed to delete chunk file: {:?}", chunk_path))?;

            current_used_space_bytes.fetch_sub(size, Ordering::SeqCst);
            current_chunk_count.fetch_sub(1, Ordering::SeqCst);
            log::info!(
                "Deleted chunk {}: {} bytes. New usage: {} bytes, {} chunks.",
                chunk_id,
                size,
                current_used_space_bytes.load(Ordering::SeqCst),
                current_chunk_count.load(Ordering::SeqCst)
            );
            // Maybe try to remove empty parent dirs here? Optional optimization.
            Ok(Some(size))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::warn!("Attempted to delete non-existent chunk: {}", chunk_id);
            Ok(None) // It's gone, so treat as success from backend's POV
        }
        Err(e) => Err(anyhow!("Error getting metadata for chunk {} to delete: {}", chunk_id, e)),
    }
}

// Get current status based on atomics
pub fn get_current_node_status(
    current_used_space_bytes: &Arc<AtomicU64>,
    max_storage_bytes: &Arc<AtomicU64>,
    current_chunk_count: &Arc<AtomicU64>,
) -> NodeStatus {
    let used = current_used_space_bytes.load(Ordering::Relaxed);
    let max_cap = max_storage_bytes.load(Ordering::Relaxed);
    NodeStatus {
        used_space_bytes: used,
        max_space_bytes: max_cap,
        free_space_bytes: max_cap.saturating_sub(used),
        chunk_count: current_chunk_count.load(Ordering::Relaxed),
    }
}