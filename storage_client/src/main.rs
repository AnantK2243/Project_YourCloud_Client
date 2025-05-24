// src/main.rs

use anyhow::{Context, Result, anyhow};
use log::{info, error};
use tokio::signal;
use storage_client::config::load_config;
use storage_client::network::start_communication_loop;
use storage_client::storage::initialize_storage_state;
use std::path::Path;

fn get_disk_available_space(storage_path: &Path) -> Result<u64> {
    use sysinfo::{System, Disks};

    let _sys = System::new_all();
    let disks = Disks::new_with_refreshed_list();

    let canonical_storage_path = storage_path.canonicalize().with_context(|| format!("Failed to canonicalize storage path for disk space check: {:?}", storage_path))?;

    let mut longest_mount_point_len = 0;
    let mut best_match_disk = None;

    for disk in &disks {
        let mount_point = disk.mount_point();
        if canonical_storage_path.starts_with(mount_point) {
            if mount_point.as_os_str().len() > longest_mount_point_len {
                longest_mount_point_len = mount_point.as_os_str().len();
                best_match_disk = Some(disk);
            }
        }
    }

    if let Some(disk) = best_match_disk {
        info!("Storage path {:?} is on disk mounted at {:?} (Disk: {:?}, Type: {:?}, Total: {} bytes, Available: {} bytes)",
            canonical_storage_path,
            disk.mount_point(),
            disk.name(),
            disk.file_system(),
            disk.total_space(),
            disk.available_space()
        );
        Ok(disk.available_space())
    } else {
        Err(anyhow!("Could not find a disk corresponding to the storage path: {:?}. Please ensure the path is valid and on a mounted partition.", storage_path))
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();


    let config = load_config().await.context("Failed to load configuration")?;
    info!("Configuration loaded successfully: {:?}", config);

    // Ensure storage path parent directory exists for disk space check if canonicalize needs it
    if let Some(parent) = config.storage_path.parent() {
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await.with_context(|| format!("Failed to create parent directory for storage path: {:?}", parent))?;
            info!("Created parent directory for storage: {:?}", parent);
        }
    }


    // Check available system disk space
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
            info!("Sufficient disk space available. System has {} bytes, configured max is {} bytes.", available_system_space, configured_max_bytes);
        }
        Err(e) => {
            error!("Could not verify available disk space: {}. Please check storage_path configuration and permissions.", e);
            return Err(e.context("Failed to verify available disk space."));
        }
    }

    // Init storage
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

    // Start the websocket
    info!("Starting communication loop...");
    if let Err(e) = start_communication_loop(&config, used_space_bytes, max_storage_bytes, chunk_count).await {
        error!("Error in communication loop: {}", e);
    }


    // Handle shutdown signal
    signal::ctrl_c().await.context("Failed to listen for ctrl_c signal")?;
    info!("Shutdown signal received. Exiting...");

    Ok(())
}





