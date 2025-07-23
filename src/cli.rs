// src/cli.rs

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use log::info;
use std::io::{self, Write};
use std::path::PathBuf;

use crate::config::{load_config, save_config, Config};
use crate::storage::{get_disk_available_space, initialize_storage_state};

#[derive(Parser)]
#[command(name = "storage_client")]
#[command(about = "Project YourCloud Storage Client - A distributed storage node daemon")]
#[command(version = "0.1.0")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the storage client in daemon mode (default operation)
    Start,
    /// Display current storage node status and configuration
    Status,
    /// Interactive setup to configure the storage node
    Setup,
    /// Validate the current configuration
    Validate,
    /// Show configuration file path and contents
    Config,
}

pub async fn handle_command(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Start => {
            info!("Starting storage client daemon...");
            // Return Ok(()) to indicate we should proceed with normal startup
            Ok(())
        }
        Commands::Status => show_status().await,
        Commands::Setup => interactive_setup().await,
        Commands::Validate => validate_configuration().await,
        Commands::Config => show_configuration().await,
    }
}

async fn show_status() -> Result<()> {
    println!("=== Project YourCloud Storage Client Status ===");

    match load_config().await {
        Ok(config) => {
            println!("Configuration loaded successfully");
            println!(
                "Node ID: {}",
                if config.node_id.is_empty() {
                    "Not configured"
                } else {
                    &config.node_id
                }
            );
            println!(
                "Auth Token: {}",
                if config.auth_token.is_empty() {
                    "Not configured"
                } else {
                    "Configured"
                }
            );
            println!("Storage Path: {:?}", config.storage_path);
            println!("Max Storage: {:.2} GiB", config.max_storage_gib);
            println!(
                "WebSocket URL: {}",
                if config.ws_url.is_empty() {
                    "Default (wss://wss.project-yourcloud.me)"
                } else {
                    &config.ws_url
                }
            );

            // Check storage path
            if config.storage_path.exists() {
                println!("Storage path exists");
            } else {
                println!("Storage path does not exist (will be created on start)");
            }

            // Check disk space
            match get_disk_available_space(&config.storage_path) {
                Ok(available_bytes) => {
                    let available_gib = available_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                    let configured_bytes =
                        (config.max_storage_gib * 1024.0 * 1024.0 * 1024.0) as u64;

                    println!("Available disk space: {:.2} GiB", available_gib);

                    if available_bytes >= configured_bytes {
                        println!("Sufficient disk space for configured storage");
                    } else {
                        println!("Insufficient disk space for configured storage");
                        println!(
                            "Required: {:.2} GiB, Available: {:.2} GiB",
                            config.max_storage_gib, available_gib
                        );
                    }
                }
                Err(e) => {
                    println!("Could not check disk space: {}", e);
                }
            }

            // Check current storage usage if path exists
            if config.storage_path.exists() {
                match initialize_storage_state(&config.storage_path, config.max_storage_gib).await {
                    Ok((used_space, max_space, chunk_count)) => {
                        let used_bytes = used_space.load(std::sync::atomic::Ordering::Relaxed);
                        let max_bytes = max_space.load(std::sync::atomic::Ordering::Relaxed);
                        let chunks = chunk_count.load(std::sync::atomic::Ordering::Relaxed);

                        let used_gib = used_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                        let max_gib = max_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                        let usage_percent = if max_bytes > 0 {
                            (used_bytes as f64 / max_bytes as f64) * 100.0
                        } else {
                            0.0
                        };

                        println!();
                        println!("=== Current Storage Usage ===");
                        println!("  Used Space: {:.2} GiB ({:.1}%)", used_gib, usage_percent);
                        println!("  Max Space: {:.2} GiB", max_gib);
                        println!("  Stored Chunks: {}", chunks);
                    }
                    Err(e) => {
                        println!("Could not analyze current storage: {}", e);
                    }
                }
            }

            // Configuration readiness check
            println!();
            if config.node_id.is_empty() || config.auth_token.is_empty() {
                println!("Node is NOT ready to start");
                println!("Missing node_id and/or auth_token");
                println!(
                    "Run 'yourcloud_client setup' to configure, register through the web interface"
                );
            } else {
                println!("Node is ready to start");
                println!("Run 'yourcloud_client start' to begin daemon mode");
            }
        }
        Err(e) => {
            println!("Failed to load configuration: {}", e);
            println!("Run 'yourcloud_client setup' to create initial configuration");
            return Err(e);
        }
    }

    Ok(())
}

async fn interactive_setup() -> Result<()> {
    println!("=== Project YourCloud Storage Client Setup ===");
    println!();

    // Load existing config or create default
    let mut config = match load_config().await {
        Ok(config) => {
            println!("Found existing configuration. Current values will be shown in [brackets].");
            config
        }
        Err(_) => {
            println!("No existing configuration found. Creating new configuration.");
            Config::default()
        }
    };

    println!();

    // Node ID
    print!("Node ID");
    if !config.node_id.is_empty() {
        print!(" [{}]", config.node_id);
    }
    print!(": ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input_str = input.trim();
    if !input_str.is_empty() {
        config.node_id = input_str.to_string();
    }

    // Auth Token
    print!("Auth Token");
    if !config.auth_token.is_empty() {
        print!(" [configured]");
    }
    print!(": ");
    io::stdout().flush()?;

    input.clear();
    io::stdin().read_line(&mut input)?;
    let input_str = input.trim();
    if !input_str.is_empty() {
        config.auth_token = input_str.to_string();
    }

    // Storage Path
    print!("Storage Path [{}]: ", config.storage_path.display());
    io::stdout().flush()?;

    input.clear();
    io::stdin().read_line(&mut input)?;
    let input_str = input.trim();
    if !input_str.is_empty() {
        config.storage_path = PathBuf::from(input_str);
    }

    // Max Storage
    print!("Maximum Storage (GiB) [{}]: ", config.max_storage_gib);
    io::stdout().flush()?;

    input.clear();
    io::stdin().read_line(&mut input)?;
    let input_str = input.trim();
    if !input_str.is_empty() {
        match input_str.parse::<f64>() {
            Ok(value) if value > 0.0 => {
                config.max_storage_gib = value;
            }
            Ok(_) => {
                return Err(anyhow!("Maximum storage must be greater than 0"));
            }
            Err(_) => {
                return Err(anyhow!("Invalid number format for maximum storage"));
            }
        }
    }

    // WebSocket URL
    print!("WebSocket URL");
    if !config.ws_url.is_empty() {
        print!(" [{}]", config.ws_url);
    } else {
        print!(" [default: wss://wss.project-yourcloud.me]");
    }
    print!(": ");
    io::stdout().flush()?;

    input.clear();
    io::stdin().read_line(&mut input)?;
    let input_str = input.trim();
    if !input_str.is_empty() {
        config.ws_url = input_str.to_string();
    }

    println!();
    println!("=== Configuration Summary ===");
    println!("Node ID: {}", config.node_id);
    println!(
        "Auth Token: {}",
        if config.auth_token.is_empty() {
            "Not configured"
        } else {
            "Configured"
        }
    );
    println!("Storage Path: {:?}", config.storage_path);
    println!("Max Storage: {:.2} GiB", config.max_storage_gib);
    println!(
        "WebSocket URL: {}",
        if config.ws_url.is_empty() {
            "Default"
        } else {
            &config.ws_url
        }
    );

    print!("\nSave this configuration? [Y/n]: ");
    io::stdout().flush()?;

    input.clear();
    io::stdin().read_line(&mut input)?;
    let input_str = input.trim().to_lowercase();

    if input_str == "n" || input_str == "no" {
        println!("Configuration not saved.");
        return Ok(());
    }

    // Save configuration
    save_config(&config)
        .await
        .context("Failed to save configuration")?;

    println!("Configuration saved successfully!");

    if config.node_id.is_empty() || config.auth_token.is_empty() {
        println!();
        println!("Note: Node ID and Auth Token are required to connect to the backend.");
        println!("You can obtain these by registering your node through the web interface.");
        println!("Then run 'yourcloud_client setup' again to configure them.");
    } else {
        println!();
        println!("Your storage node is ready to start!");
        println!("Run 'yourcloud_client start' to begin daemon mode.");
    }

    Ok(())
}

async fn validate_configuration() -> Result<()> {
    println!("=== Configuration Validation ===");

    match load_config().await {
        Ok(config) => {
            println!("Configuration file loaded successfully");

            // Validate configuration
            match crate::config::validate_config(&config) {
                Ok(_) => {
                    println!("Configuration is valid");
                }
                Err(e) => {
                    println!("Configuration validation failed: {}", e);
                    return Err(e);
                }
            }

            // Check disk space
            let configured_max_bytes = (config.max_storage_gib * 1024.0 * 1024.0 * 1024.0) as u64;
            match get_disk_available_space(&config.storage_path) {
                Ok(available_system_space) => {
                    if available_system_space >= configured_max_bytes {
                        println!("Sufficient disk space available");
                    } else {
                        println!("Insufficient disk space for configured max_storage_gib");
                        println!(
                            "Available: {} bytes, Required: {} bytes",
                            available_system_space, configured_max_bytes
                        );
                        return Err(anyhow!(
                            "Not enough disk space to meet configured max_storage_gib"
                        ));
                    }
                }
                Err(e) => {
                    println!("Could not verify available disk space: {}", e);
                    return Err(e);
                }
            }

            // Check node readiness
            if config.node_id.is_empty() || config.auth_token.is_empty() {
                println!("Node ID and/or Auth Token not configured");
                println!("Node will not be able to connect to backend without these credentials");
            } else {
                println!("Node credentials configured");
            }

            println!();
            println!("Configuration validation completed successfully");
        }
        Err(e) => {
            println!("Failed to load configuration: {}", e);
            return Err(e);
        }
    }

    Ok(())
}

async fn show_configuration() -> Result<()> {
    let config_path = crate::config::get_config_path()?;

    println!("=== Configuration Information ===");
    println!("Config file path: {:?}", config_path);

    if config_path.exists() {
        println!("Configuration file exists");

        match tokio::fs::read_to_string(&config_path).await {
            Ok(contents) => {
                println!();
                println!("Configuration file contents:");
                println!("---");
                println!("{}", contents);
                println!("---");
            }
            Err(e) => {
                println!("Failed to read configuration file: {}", e);
                return Err(e.into());
            }
        }

        // Also show parsed config
        match load_config().await {
            Ok(config) => {
                println!();
                println!("Parsed configuration:");
                println!(
                    "  Node ID: {}",
                    if config.node_id.is_empty() {
                        "Not configured"
                    } else {
                        &config.node_id
                    }
                );
                println!(
                    "  Auth Token: {}",
                    if config.auth_token.is_empty() {
                        "Not configured"
                    } else {
                        "Configured"
                    }
                );
                println!("  Storage Path: {:?}", config.storage_path);
                println!("  Max Storage: {:.2} GiB", config.max_storage_gib);
                println!(
                    "  WebSocket URL: {}",
                    if config.ws_url.is_empty() {
                        "Default (wss://wss.project-yourcloud.me)"
                    } else {
                        &config.ws_url
                    }
                );
            }
            Err(e) => {
                println!("Failed to parse configuration: {}", e);
            }
        }
    } else {
        println!("Configuration file does not exist");
        println!("Run 'yourcloud_client setup' to create initial configuration");
    }

    Ok(())
}
