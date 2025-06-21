// src/main.rs

use anyhow::{anyhow, Context, Result};
use log::{error, info};
use tokio::fs;
use tokio::signal;

// Backend WebSocket URL configuration
// const WS_URL: &str = "wss://wss.project-yourcloud.me";
const WS_URL: &str = "wss://localhost:4200";
// Declare modules
mod commands;
mod config;
mod network;
mod storage;
mod webrtc;

use crate::config::load_config;
use crate::network::start_communication_loop;
use crate::storage::get_disk_available_space;
use crate::storage::initialize_storage_state;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    info!("Starting storage client");

    // Get user config
    let config = load_config().await?;

    info!("Configuration loaded successfully");

    // Validate configuration
    info!("Ensuring storage path exists: {:?}", config.storage_path);
    fs::create_dir_all(&config.storage_path)
        .await
        .with_context(|| {
            format!(
                "Failed to create storage directory: {:?}",
                config.storage_path
            )
        })?;
    info!("Storage path exists.");

    // Check available system disk space
    let configured_max_bytes = config.max_storage_gib * 1024 * 1024 * 1024;
    // Call the function from the storage module
    match get_disk_available_space(&config.storage_path) {
        Ok(available_system_space) => {
            if available_system_space < configured_max_bytes {
                error!(
                    "Insufficient disk space for configured max_storage_gib. Available: {} bytes, Required: {} bytes for path {:?}.",
                    available_system_space, configured_max_bytes, config.storage_path
                );
                return Err(anyhow!(
                    "Not enough disk space to meet configured max_storage_gib."
                ));
            }
            info!("Sufficient disk space available.");
        }
        Err(e) => {
            error!("Could not verify available disk space: {}", e);
            return Err(e.context("Failed to verify available disk space."));
        }
    }

    // Initialize storage state (scans existing files)
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

    // Start the communication loop (includes registration if needed)
    info!("Starting communication loop...");
    let comm_loop = start_communication_loop(
        config.clone(),
        WS_URL.to_string(),
        used_space_bytes,
        max_storage_bytes,
        chunk_count,
    );

    // Handle shutdown signal
    tokio::select! {
        result = comm_loop => {
            if let Err(e) = result {
                if e.to_string().contains("Node not registered") {
                    error!("Node ID and Auth Token are required. Please register this node through the web interface first.");
                } else {
                    error!("Communication loop exited with error: {:#}", e);
                }
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
