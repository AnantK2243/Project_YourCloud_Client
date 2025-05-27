// src/storage.rs

use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio_util::codec::{FramedRead, BytesCodec};
use bytes::Bytes;
use anyhow::{Result, Context, anyhow};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use futures_util::{Stream, StreamExt, TryStreamExt};
use serde::Serialize;
use sysinfo::{System, Disks};

#[derive(Serialize, Debug, Clone)]
pub struct NodeStatus {
    pub used_space_bytes: u64,
    pub max_space_bytes: u64,
    pub free_space_bytes: u64,
    pub chunk_count: u64,
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

pub fn get_disk_available_space(storage_path: &Path) -> Result<u64> {
    // Converts the given storage path to its absolute canonical path.
    let canonical_storage_path = storage_path.canonicalize()
        .with_context(|| format!("Failed to canonicalize storage path for disk space check: {:?}. Ensure it exists.", storage_path))?;

    // Fetches a list of all disks on the system.
    let disks = Disks::new_with_refreshed_list();

    // Variables to store the longest matching mount point and the corresponding disk.
    let mut longest_mount_point_len = 0;
    let mut best_match_disk = None;

    // Iterates through each disk to find the one that the storage path resides on.
    for disk in &disks {
        let mount_point = disk.mount_point();
        // Checks if the canonical storage path starts with the current disk's mount point.
        if canonical_storage_path.starts_with(mount_point) {
            let mount_point_len = mount_point.as_os_str().len();
            // If this mount point is longer than the previous longest, it's a better match.
            if mount_point_len > longest_mount_point_len {
                longest_mount_point_len = mount_point_len;
                best_match_disk = Some(disk);
            }
        }
    }

    // If a matching disk was found.
    if let Some(disk) = best_match_disk {
        // log::info!( 
        //     "Storage path {:?} is on disk mounted at {:?} (Disk: {:?}, FS: {:?}, Total: {} bytes, Available: {} bytes)",
        //     canonical_storage_path,
        //     disk.mount_point(),
        //     disk.name(),
        //     disk.file_system(),
        //     disk.total_space(),
        //     disk.available_space()
        // );
        
        // Returns the available space on the disk.
        Ok(disk.available_space())
    } else {
        // If no matching disk was found, returns an error.
        Err(anyhow!(
            "Could not find a disk corresponding to the storage path: {:?}. Please check path and mounts.",
            storage_path
        ))
    }
}


pub async fn initialize_storage_state(storage_path_base: &Path, max_storage_gib: u64) -> Result<(Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>)> {
    let current_used_space = Arc::new(AtomicU64::new(0));
    let current_chunk_count = Arc::new(AtomicU64::new(0));
    let max_bytes = Arc::new(AtomicU64::new(max_storage_gib * 1024 * 1024 * 1024));

    // Check storage path existance
    if !storage_path_base.exists() {
        fs::create_dir_all(storage_path_base)
            .await
            .with_context(|| format!("Failed to create base storage directory: {:?}", storage_path_base))?;
        log::info!("Created base storage directory: {:?}", storage_path_base);
    }

    // Check for existing files
    log::info!("Scanning storage directory {:?} for existing chunks...", storage_path_base);
    let mut total_size = 0u64;
    let mut total_count = 0u64;
    let mut first_level_dirs = fs::read_dir(storage_path_base).await?;

    // Parse existing files
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

pub async fn store_chunk_to_disk(storage_path_base: &Path, chunk_id: &str, mut data_stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin, expected_size_bytes: u64, current_used_space_bytes: &Arc<AtomicU64>, max_storage_bytes: &Arc<AtomicU64>, current_chunk_count: &Arc<AtomicU64>) -> Result<u64> {
    // Function to add a chunk to store
    let chunk_path = get_chunk_path(storage_path_base, chunk_id);

    // Check for pre-existence
    if tokio::fs::try_exists(&chunk_path).await.unwrap_or(false) {
        log::warn!("Chunk {} already exists, refusing to overwrite.", chunk_id);
        return Err(anyhow!("Chunk {} already exists.", chunk_id));
    }

    // Check user set storage limit
    let current_max = max_storage_bytes.load(Ordering::Relaxed);
    let current_used = current_used_space_bytes.load(Ordering::Relaxed);

    if current_used.saturating_add(expected_size_bytes) > current_max {
        return Err(anyhow!(
            "Insufficient configured storage space. Required: {}, Available within limit: {}. Current used: {}/{}",
            expected_size_bytes,
            current_max.saturating_sub(current_used),
            current_used,
            current_max
        ));
    }

    // Check total available system RAM for file buffering
    let mut system = System::new_all();
    system.refresh_all();
    let available_system_space = system.available_memory() as u64;

    if expected_size_bytes > available_system_space {
        return Err(anyhow!(
            "Insufficient system memory to store chunk. Required: {}, Available: {}",
            expected_size_bytes,
            available_system_space
        ));
    }

    // Check actual available system disk space on the target partition
    match get_disk_available_space(storage_path_base) {
        Ok(available_disk_space) => {
            if expected_size_bytes > available_disk_space {
                return Err(anyhow!(
                    "Insufficient physical disk space on target partition. Required: {}, Available on disk: {}",
                    expected_size_bytes,
                    available_disk_space
                ));
            }
        }
        Err(e) => {
            log::warn!(
                "Could not reliably determine available disk space for path {:?}: {}. Proceeding with caution.",
                storage_path_base,
                e
            );
        }
    }

    // Ensure parent directory exists
    if let Some(parent_dir) = chunk_path.parent() {
        fs::create_dir_all(parent_dir).await.with_context(|| format!("Failed to create directory for chunk: {:?}", parent_dir))?;
    } else {
        return Err(anyhow!("Invalid chunk path, no parent dir: {:?}", chunk_path));
    }

    // Stream data to file
    let mut file = fs::File::create(&chunk_path).await.with_context(|| format!("Failed to create file for chunk: {:?}", chunk_path))?;
    let mut bytes_written: u64 = 0;

    while let Some(data_result) = data_stream.next().await {
        let data_bytes = data_result.context("Error streaming chunk data from source")?;
        file.write_all(&data_bytes).await.with_context(|| format!("Failed to write data to chunk file: {:?}", chunk_path))?;
        bytes_written += data_bytes.len() as u64;
    }
    file.sync_all().await.with_context(|| format!("Failed to sync chunk file: {:?}", chunk_path))?;

    // Verify size and update atomics
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

pub async fn retrieve_chunk_from_disk(storage_path_base: &Path, chunk_id: &str) -> Result<impl Stream<Item = Result<Bytes, std::io::Error>>> {
    // Function to retrieve a chunk from store
    let chunk_path = get_chunk_path(storage_path_base, chunk_id);
    log::debug!("Attempting to retrieve chunk from {:?}", chunk_path);

    // Check for chunk existence
    if !tokio::fs::try_exists(&chunk_path).await.unwrap_or(false) {
        return Err(anyhow!("Chunk {} not found at {:?}", chunk_id, chunk_path));
    }

    // Access file store, error on failure
    let file = fs::File::open(&chunk_path).await.with_context(|| format!("Failed to open chunk file: {:?}", chunk_path))?;
    let reader = BufReader::new(file);
    let stream = FramedRead::new(reader, BytesCodec::new()).map_ok(|bytes| bytes.freeze());
    Ok(stream)
}

pub async fn delete_chunk_from_disk(storage_path_base: &Path, chunk_id: &str, current_used_space_bytes: &Arc<AtomicU64>, current_chunk_count: &Arc<AtomicU64>) -> Result<Option<u64>> {
    // Function to delete a chunk from store
    let chunk_path = get_chunk_path(storage_path_base, chunk_id);
    
    match fs::metadata(&chunk_path).await {
        Ok(metadata) => {
            let size = metadata.len();
            // Delete the chunk from the store
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

            // Attempt to remove empty parent directories
            if chunk_id.len() >= 4 { // Only if subdir1/subdir2 structure was used
                if let Some(parent_dir) = chunk_path.parent() { // This is base_path/subdir1/subdir2
                    match remove_dir_if_empty(parent_dir).await {
                        Ok(true) => { // parent_dir (subdir2) was removed
                            log::debug!("Removed empty shard directory: {:?}", parent_dir);
                            // If subdir2 was removed, try to remove subdir1
                            if let Some(grandparent_dir) = parent_dir.parent() { // This is base_path/subdir1
                                // Ensure we don't try to remove the storage_path_base itself
                                if grandparent_dir != storage_path_base {
                                    match remove_dir_if_empty(grandparent_dir).await {
                                        Ok(true) => log::debug!("Removed empty shard directory: {:?}", grandparent_dir),
                                        Ok(false) => { /* Grandparent not empty or not removed, do nothing */ }
                                        Err(e) => log::warn!("Failed to attempt removal of grandparent directory {:?}: {}", grandparent_dir, e),
                                    }
                                }
                            }
                        }
                        Ok(false) => { /* Parent not empty or not removed, do nothing */ }
                        Err(e) => {
                            log::warn!("Failed to attempt removal of parent directory {:?}: {}", parent_dir, e);
                        }
                    }
                }
            }
            Ok(Some(size))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::warn!("Attempted to delete non-existent chunk: {}", chunk_id);
            Ok(None)
        }
        Err(e) => Err(anyhow!("Error getting metadata for chunk {} to delete: {}", chunk_id, e)),
    }
}

#[allow(non_snake_case)]
async fn remove_dir_if_empty(dir_path: &Path) -> Result<bool> {
    // Helper function to remove a directory if it's empty
    match fs::read_dir(dir_path).await {
        Ok(mut entries) => {
            match entries.next_entry().await {
                // Directory is empty
                Ok(None) => {
                    match fs::remove_dir(dir_path).await {
                        Ok(_) => {
                            // log::debug!("Successfully removed empty directory: {:?}", dir_path); // Logged by caller
                            Ok(true)
                        }
                        Err(e) => {
                            // Failed to remove, treat as not removed for the caller
                            log::warn!("Failed to remove supposedly empty directory {:?}: {}. It might have been repopulated or there are permission issues.", dir_path, e);
                            Ok(false)
                        }
                    }
                }
                // Directory is not empty
                Ok(Some(_)) => { 
                    log::trace!("Directory {:?} is not empty, not removing.", dir_path);
                    Ok(false)
                }
                Err(e) => {
                    // Error trying to read the first entry
                    Err(anyhow!(e).context(format!("Error checking if directory {:?} is empty (reading first entry)", dir_path)))
                }
            }
        }
        Err(e) => {
            // Directory not found
            if e.kind() == std::io::ErrorKind::NotFound {
                log::debug!("Directory {:?} not found, cannot remove if empty (already gone).", dir_path);
                Ok(false)
            } else {
                // Error trying to read the directory itself
                Err(anyhow!(e).context(format!("Error reading directory {:?}", dir_path)))
            }
        }
    }
}

pub fn get_current_node_status(current_used_space_bytes: &Arc<AtomicU64>, max_storage_bytes: &Arc<AtomicU64>, current_chunk_count: &Arc<AtomicU64>) -> NodeStatus {
    // Return the current status of the file store
    let used = current_used_space_bytes.load(Ordering::Relaxed);
    let max_cap = max_storage_bytes.load(Ordering::Relaxed);
    NodeStatus {
        used_space_bytes: used,
        max_space_bytes: max_cap,
        free_space_bytes: max_cap.saturating_sub(used),
        chunk_count: current_chunk_count.load(Ordering::Relaxed),
    }
}