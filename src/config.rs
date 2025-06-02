// src/config.rs

use anyhow::{Context, Result};
use dirs;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;
use tokio::fs;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub node_id: String,
    pub auth_token: String,
    pub storage_path: PathBuf,
    pub max_storage_gib: u64,
}

impl Default for Config {
    fn default() -> Self {
        info!("Creating default configuration");

        // Generate default user storage folder
        let default_storage_path = dirs::home_dir()
            .map(|mut path| {
                path.push("Project_YourCloud");
                info!("Using home directory for default storage: {:?}", path);
                path
            })
            .or_else(|| {
                env::var("USER")
                    .ok()
                    .map(|user_name| {
                        let path = PathBuf::from(format!("/home/{}/Project_YourCloud", user_name));
                        info!("Using USER env var for storage path: {:?}", path);
                        path
                    })
            })
            .unwrap_or_else(|| {
                // Dump to working directory
                warn!("Could not determine home directory or USER env var. Defaulting storage_path to current directory + 'Project_YourCloud'.");
                let fallback_path = std::env::current_dir()
                    .unwrap_or_else(|_| PathBuf::from("/tmp"))
                    .join("Project_YourCloud");
                info!("Fallback storage path: {:?}", fallback_path);
                fallback_path
            });

        info!(
            "Default config created with storage path: {:?}",
            default_storage_path
        );

        Self {
            node_id: String::new(),
            auth_token: String::new(),
            storage_path: default_storage_path,
            max_storage_gib: 20,
        }
    }
}

pub async fn load_config() -> Result<Config> {
    let config_path = get_config_path()?;
    info!("Loading configuration from: {:?}", config_path);

    // First time run, create default config file at path
    if !config_path.exists() {
        info!(
            "Config file not found, creating default at: {:?}",
            config_path
        );
        let default_config = Config::default();
        save_config(&default_config).await?;
        info!("Default config file created successfully");

        // Return default config
        return Ok(default_config);
    }

    info!("Config file found, reading from: {:?}", config_path);

    // Read and return
    let config_content = fs::read_to_string(&config_path)
        .await
        .context("Failed to read config file")?;

    debug!("Config file size: {} bytes", config_content.len());

    #[derive(Deserialize)]
    struct FileConfig {
        pub node_id: String,
        pub auth_token: String,
        pub storage_path: PathBuf,
        pub max_storage_gib: u64,
    }

    let file_config: FileConfig =
        toml::from_str(&config_content).context("Failed to parse config file")?;

    info!("Config file parsed successfully");
    debug!(
        "Loaded config - node_id: {}, storage_path: {:?}, max_storage: {} GiB",
        file_config.node_id, file_config.storage_path, file_config.max_storage_gib
    );

    let config = Config {
        node_id: file_config.node_id,
        auth_token: file_config.auth_token,
        storage_path: file_config.storage_path,
        max_storage_gib: file_config.max_storage_gib,
    };

    validate_config(&config)?;
    info!("Configuration validation passed");
    Ok(config)
}

pub async fn save_config(config: &Config) -> Result<()> {
    let config_path = get_config_path()?;
    info!("Saving configuration to: {:?}", config_path);

    // Create parent directories if they don't exist
    if let Some(parent) = config_path.parent() {
        debug!("Creating parent directories: {:?}", parent);
        fs::create_dir_all(parent)
            .await
            .context("Failed to create config directory")?;
        debug!("Parent directories created successfully");
    }

    #[derive(Serialize)]
    struct SaveConfig {
        pub node_id: String,
        pub auth_token: String,
        pub storage_path: PathBuf,
        pub max_storage_gib: u64,
    }

    let save_config = SaveConfig {
        node_id: config.node_id.clone(),
        auth_token: config.auth_token.clone(),
        storage_path: config.storage_path.clone(),
        max_storage_gib: config.max_storage_gib,
    };

    debug!("Serializing config to TOML");
    let toml_string =
        toml::to_string_pretty(&save_config).context("Failed to serialize config to TOML")?;

    debug!("Writing config file ({} bytes)", toml_string.len());
    fs::write(&config_path, toml_string)
        .await
        .context("Failed to write config file")?;

    info!("Configuration saved successfully to: {:?}", config_path);
    Ok(())
}

fn get_config_path() -> Result<PathBuf> {
    // Return default user config path or store in /etc directory
    let config_path = dirs::config_dir()
        .map(|dir| {
            let path = dir.join("Project_YourCloud").join("config.toml");
            debug!("Using user config directory: {:?}", path);
            path
        })
        .unwrap_or_else(|| {
            let path = PathBuf::from("/etc/Project_YourCloud/config.toml");
            debug!("Falling back to system config directory: {:?}", path);
            path
        });

    debug!("Config path determined: {:?}", config_path);
    Ok(config_path)
}

fn validate_config(config: &Config) -> Result<()> {
    debug!("Validating configuration");

    // Non-absolute Path
    if !config.storage_path.is_absolute() {
        error!(
            "Invalid storage_path: must be absolute, got: {:?}",
            config.storage_path
        );
        return Err(anyhow::anyhow!(
            "storage_path must be an absolute path. Current: {:?}",
            config.storage_path
        ));
    }
    debug!("Storage path validation passed: {:?}", config.storage_path);

    // Config storage misconfigured
    if config.max_storage_gib <= 0 {
        error!(
            "Invalid max_storage_gib: must be > 0, got: {}",
            config.max_storage_gib
        );
        return Err(anyhow::anyhow!("max_storage_gib must be greater than 0"));
    }
    debug!(
        "Max storage validation passed: {} GiB",
        config.max_storage_gib
    );

    debug!("Configuration validation completed successfully");
    Ok(())
}
