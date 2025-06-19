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

pub async fn initialize_storage_state(
    storage_path_base: &Path,
    max_storage_gib: u64,
) -> Result<(Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>)> {
    let current_used_space = Arc::new(AtomicU64::new(0));
    let current_chunk_count = Arc::new(AtomicU64::new(0));
    let max_bytes = Arc::new(AtomicU64::new(max_storage_gib * 1024 * 1024 * 1024));

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

    current_used_space.store(total_size, Ordering::SeqCst);
    current_chunk_count.store(total_count, Ordering::SeqCst);

    info!(
        "Storage scan complete: Used: {} bytes, Max: {} bytes, Chunks: {}",
        total_size,
        max_bytes.load(Ordering::SeqCst),
        total_count
    );
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
    let chunk_path = storage_path_base.join(chunk_id);

    // Check for pre-existence
    if tokio::fs::try_exists(&chunk_path).await.unwrap_or(false) {
        warn!("Chunk {} already exists, refusing to overwrite.", chunk_id);
        return Err(anyhow!("Chunk {} already exists.", chunk_id));
    }

    let chunk_size = chunk_data.len() as u64;

    // Check user set storage limit
    let current_max = max_storage_bytes.load(Ordering::Relaxed);
    let current_used = current_used_space_bytes.load(Ordering::Relaxed);

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

    // Write chunk data to file
    fs::write(&chunk_path, chunk_data)
        .await
        .with_context(|| format!("Failed to write chunk {} to disk", chunk_id))?;

    // Update counters
    current_used_space_bytes.fetch_add(chunk_size, Ordering::Relaxed);
    current_chunk_count.fetch_add(1, Ordering::Relaxed);

    info!(
        "Successfully stored chunk {} ({} bytes)",
        chunk_id, chunk_size
    );
    Ok(chunk_size)
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

    info!(
        "Successfully retrieved chunk {} ({} bytes)",
        chunk_id,
        chunk_data.len()
    );
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

            current_used_space_bytes.fetch_sub(size, Ordering::SeqCst);
            current_chunk_count.fetch_sub(1, Ordering::SeqCst);
            info!(
                "Deleted chunk {}: {} bytes. New usage: {} bytes, {} chunks.",
                chunk_id,
                size,
                current_used_space_bytes.load(Ordering::SeqCst),
                current_chunk_count.load(Ordering::SeqCst)
            );

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
