// src/main.rs

use anyhow::{Context, Result, anyhow};
use log::{info, error};
use std::path::Path;
use sysinfo::Disks;
use tokio::fs;
use tokio::signal;

// Declare modules
mod config;
mod network;
mod storage;
mod commands;

use crate::config::load_config;
use crate::network::start_communication_loop;
use crate::storage::initialize_storage_state;

fn get_disk_available_space(storage_path: &Path) -> Result<u64> {
    // Must run after storage_path is created
    let canonical_storage_path = storage_path.canonicalize()
        .with_context(|| format!("Failed to canonicalize storage path for disk space check: {:?}. Ensure it exists.", storage_path))?;

    let disks = Disks::new_with_refreshed_list();

    let mut longest_mount_point_len = 0;
    let mut best_match_disk = None;

    for disk in &disks {
        let mount_point = disk.mount_point();
        if canonical_storage_path.starts_with(mount_point) {
            let mount_point_len = mount_point.as_os_str().len();
            if mount_point_len > longest_mount_point_len {
                longest_mount_point_len = mount_point_len;
                best_match_disk = Some(disk);
            }
        }
    }

    if let Some(disk) = best_match_disk {
        info!(
            "Storage path {:?} is on disk mounted at {:?} (Disk: {:?}, FS: {:?}, Total: {} bytes, Available: {} bytes)",
            canonical_storage_path,
            disk.mount_point(),
            disk.name(),
            disk.file_system(),
            disk.total_space(),
            disk.available_space()
        );
        Ok(disk.available_space())
    } else {
        Err(anyhow!(
            "Could not find a disk corresponding to the storage path: {:?}. Please check path and mounts.",
            storage_path
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let initial_config_result = load_config().await;
    let log_level = initial_config_result
        .as_ref()
        .map(|c| c.log_level.clone())
        .unwrap_or_else(|_| "info".to_string());

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&log_level)).init();

    // 2. Load configuration (or handle error)
    let config = initial_config_result.context("Failed to load configuration")?;
    info!("Configuration loaded."); // Avoid logging the config itself

    // 3. Ensure storage path exists BEFORE checking disk space
    info!("Ensuring storage path exists: {:?}", config.storage_path);
    fs::create_dir_all(&config.storage_path)
        .await
        .with_context(|| format!("Failed to create storage directory: {:?}", config.storage_path))?;
    info!("Storage path exists.");


    // 4. Check available system disk space
    let configured_max_bytes = config.max_storage_gib * 1024 * 1024 * 1024;
    match get_disk_available_space(&config.storage_path) {
        Ok(available_system_space) => {
            if available_system_space < configured_max_bytes {
                error!(
                    "Insufficient disk space for configured max_storage_gib. Available: {} bytes, Required: {} bytes for path {:?}.",
                    available_system_space, configured_max_bytes, config.storage_path
                );
                return Err(anyhow!("Not enough disk space to meet configured max_storage_gib."));
            }
            info!("Sufficient disk space available.");
        }
        Err(e) => {
            error!("Could not verify available disk space: {}", e);
            return Err(e.context("Failed to verify available disk space."));
        }
    }

    // 5. Initialize storage state (scans existing files)
    let (used_space_bytes, max_storage_bytes, chunk_count) =
        initialize_storage_state(&config.storage_path, config.max_storage_gib)
            .await
            .context("Failed to initialize storage state")?;
    info!(
        "Storage state initialized. Max: {} bytes, Current Used: {} bytes, Chunks: {}.",
        max_storage_bytes.load(std::sync::atomic::Ordering::Relaxed),
        used_space_bytes.load(std::sync::atomic::Ordering::Relaxed),
        chunk_count.load(std::sync::atomic::Ordering::Relaxed)
    );

    // 6. Start the communication loop (includes registration if needed)
    info!("Starting communication loop...");
    let comm_loop = start_communication_loop(
        config.clone(), // Pass a clone as start_comm_loop might modify config (during registration)
        used_space_bytes,
        max_storage_bytes,
        chunk_count
    );

    // 7. Handle shutdown signal
    tokio::select! {
        result = comm_loop => {
            if let Err(e) = result {
                error!("Communication loop exited with error: {:#}", e);
                return Err(e);
            } else {
                 info!("Communication loop exited gracefully.");
            }
        }
        _ = signal::ctrl_c() => {
            info!("Shutdown signal received. Exiting...");
        }
    }

    Ok(())
}