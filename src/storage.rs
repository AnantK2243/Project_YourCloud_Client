// src/storage.rs

use anyhow::{anyhow, Context, Result};
use log::{error, info, warn};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use sysinfo::Disks;
use tokio::fs;

pub fn get_disk_available_space(storage_path: &Path) -> Result<u64> {
    // Converts the given storage path to its absolute canonical path.
    let canonical_storage_path = storage_path.canonicalize().with_context(|| {
        format!(
            "Failed to canonicalize storage path for disk space check: {:?}. Ensure it exists.",
            storage_path
        )
    })?;

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

pub async fn initialize_storage_state(
    storage_path_base: &Path,
    max_storage_gib: f64,
) -> Result<(Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>)> {
    let current_used_space = Arc::new(AtomicU64::new(0));
    let current_chunk_count = Arc::new(AtomicU64::new(0));
    let max_bytes = Arc::new(AtomicU64::new(
        (max_storage_gib * 1024.0 * 1024.0 * 1024.0) as u64,
    ));

    // Check storage path existance
    if !storage_path_base.exists() {
        fs::create_dir_all(storage_path_base)
            .await
            .with_context(|| {
                format!(
                    "Failed to create base storage directory: {:?}",
                    storage_path_base
                )
            })?;
        info!("Created base storage directory: {:?}", storage_path_base);
    }

    // Check for existing files
    info!(
        "Scanning storage directory {:?} for existing chunks...",
        storage_path_base
    );
    let mut total_size = 0u64;
    let mut total_count = 0u64;
    let mut chunk_files = fs::read_dir(storage_path_base).await?;

    // Parse existing files
    while let Some(chunk_entry) = chunk_files.next_entry().await? {
        let chunk_path = chunk_entry.path();
        if chunk_path.is_file() {
            match fs::metadata(&chunk_path).await {
                Ok(metadata) => {
                    total_size += metadata.len();
                    total_count += 1;
                }
                Err(e) => {
                    warn!("Failed to get metadata for file {:?}: {}", chunk_path, e);
                }
            }
        }
    }

    current_used_space.store(total_size, Ordering::Release);
    current_chunk_count.store(total_count, Ordering::Release);

    Ok((current_used_space, max_bytes, current_chunk_count))
}

pub async fn store_chunk_data_to_disk(
    storage_path_base: &Path,
    chunk_id: &str,
    chunk_data: &[u8],
    current_used_space_bytes: &Arc<AtomicU64>,
    max_storage_bytes: &Arc<AtomicU64>,
    current_chunk_count: &Arc<AtomicU64>,
) -> Result<u64> {
    // Validate chunk_id for security
    if chunk_id.is_empty() || chunk_id.len() > 255 {
        return Err(anyhow!("Invalid chunk_id: empty or too long"));
    }

    if chunk_id.contains("..") || chunk_id.contains("/") || chunk_id.contains("\\") {
        return Err(anyhow!(
            "Invalid chunk_id: contains path traversal characters"
        ));
    }

    let chunk_path = storage_path_base.join(chunk_id);
    let chunk_size = chunk_data.len() as u64;

    // Validate chunk size is reasonable
    if chunk_size > 1024 * 1024 * 1024 {
        return Err(anyhow!("Chunk too large: {} bytes", chunk_size));
    }

    // Check user set storage limit with overflow protection
    let current_max = max_storage_bytes.load(Ordering::Acquire);
    let current_used = current_used_space_bytes.load(Ordering::Acquire);

    if current_used.saturating_add(chunk_size) > current_max {
        return Err(anyhow!(
            "Insufficient configured storage space. Required: {}, Available within limit: {}. Current used: {}/{}",
            chunk_size,
            current_max.saturating_sub(current_used),
            current_used,
            current_max
        ));
    }

    // Check actual available system disk space
    match get_disk_available_space(storage_path_base) {
        Ok(available_disk_space) => {
            if chunk_size > available_disk_space {
                return Err(anyhow!(
                    "Insufficient disk space. Required: {}, Available: {}",
                    chunk_size,
                    available_disk_space
                ));
            }
        }
        Err(e) => {
            error!("Could not verify disk space: {}", e);
            return Err(e);
        }
    }

    // Use a temporary file and rename to ensure atomicity
    let temp_path = storage_path_base.join(format!(".tmp_{}", chunk_id));

    // Clean up any existing temp file
    if let Err(e) = fs::remove_file(&temp_path).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!(
                "Failed to clean up existing temp file {:?}: {}",
                temp_path, e
            );
        }
    }

    // Write to temporary file first
    match fs::write(&temp_path, chunk_data).await {
        Ok(_) => {
            // Atomically move temp file to final location
            match fs::rename(&temp_path, &chunk_path).await {
                Ok(_) => {
                    // Update counters only after successful write
                    current_used_space_bytes.fetch_add(chunk_size, Ordering::Acquire);
                    current_chunk_count.fetch_add(1, Ordering::Acquire);
                    Ok(chunk_size)
                }
                Err(e) => {
                    // Clean up temp file if rename failed
                    let _ = fs::remove_file(&temp_path).await;

                    if e.kind() == std::io::ErrorKind::AlreadyExists {
                        Err(anyhow!("Chunk {} already exists", chunk_id))
                    } else {
                        Err(anyhow!("Failed to finalize chunk write: {}", e))
                    }
                }
            }
        }
        Err(e) => {
            // Clean up temp file if write failed
            let _ = fs::remove_file(&temp_path).await;
            Err(anyhow!("Failed to write chunk {} to disk: {}", chunk_id, e))
        }
    }
}

pub async fn retrieve_chunk_data_from_disk(
    storage_path_base: &Path,
    chunk_id: &str,
) -> Result<Vec<u8>> {
    let chunk_path = storage_path_base.join(chunk_id);

    // Check if chunk exists
    if !tokio::fs::try_exists(&chunk_path).await.unwrap_or(false) {
        return Err(anyhow!("Chunk {} not found", chunk_id));
    }

    // Read chunk data
    let chunk_data = fs::read(&chunk_path)
        .await
        .with_context(|| format!("Failed to read chunk {} from disk", chunk_id))?;

    Ok(chunk_data)
}

pub async fn delete_chunk_from_disk(
    storage_path_base: &Path,
    chunk_id: &str,
    current_used_space_bytes: &Arc<AtomicU64>,
    current_chunk_count: &Arc<AtomicU64>,
) -> Result<Option<u64>> {
    let chunk_path = storage_path_base.join(chunk_id);

    match fs::metadata(&chunk_path).await {
        Ok(metadata) => {
            let size = metadata.len();
            // Delete the chunk from the store
            fs::remove_file(&chunk_path)
                .await
                .with_context(|| format!("Failed to delete chunk file: {:?}", chunk_path))?;

            current_used_space_bytes.fetch_sub(size, Ordering::Release);
            current_chunk_count.fetch_sub(1, Ordering::Release);

            Ok(Some(size))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            warn!("Attempted to delete non-existent chunk: {}", chunk_id);
            Ok(None)
        }
        Err(e) => Err(anyhow!(
            "Error getting metadata for chunk {} to delete: {}",
            chunk_id,
            e
        )),
    }
}

pub async fn check_chunk_exists(storage_path_base: &Path, chunk_id: &str) -> Result<bool> {
    let chunk_path = storage_path_base.join(chunk_id);
    match tokio::fs::try_exists(&chunk_path).await {
        Ok(exists) => Ok(exists),
        Err(e) => Err(anyhow!(
            "Error checking existence of chunk {}: {}",
            chunk_id,
            e
        )),
    }
}
