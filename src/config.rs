// src/config.rs

use anyhow::{anyhow, Context, Result};
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
    pub max_storage_gib: f64,
    pub ws_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            auth_token: String::new(),
            storage_path: get_default_storage_path(),
            max_storage_gib: 40.0,
            ws_url: String::new(),
        }
    }
}

pub async fn load_config() -> Result<Config> {
    let config_path = get_config_path()?;
    info!("Attempting to load configuration from: {:?}", config_path);

    // TODO: REMOVE, NOW HANDLED MANUALLY
    // First time run, create default config file at path
    // if !config_path.exists() {
    //     warn!(
    //         "Config file not found, creating default at: {:?}",
    //         config_path
    //     );
    //     let default_config = Config::default();
    //     save_config(&default_config).await?;

    //     // Display warning and stop the program
    //     info!("DEFAULT CONFIGURATION CREATED, PLEASE RUN WITH SETUP COMMAND TO CONFIGURE");

    //     return Err(anyhow!("Default configuration created. Please configure node_id and auth_token before running again."));
    // }

    if !config_path.exists() {
        info!("Configuration file does not exist, please run the setup command to configure");
        return Err(anyhow!(
            "Configuration file does not exist. Please run the setup command to create it."
        ));
    }

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
        pub max_storage_gib: f64,
        pub ws_url: String,
    }

    let file_config: FileConfig =
        toml::from_str(&config_content).context("Failed to parse config file")?;

    debug!(
        "Loaded config - node_id: {}, storage_path: {:?}, max_storage: {} GiB",
        file_config.node_id, file_config.storage_path, file_config.max_storage_gib
    );

    let config = Config {
        node_id: file_config.node_id,
        auth_token: file_config.auth_token,
        storage_path: file_config.storage_path,
        max_storage_gib: file_config.max_storage_gib,
        ws_url: file_config.ws_url,
    };

    validate_config(&config)?;
    Ok(config)
}

fn get_default_storage_path() -> PathBuf {
    // Default storage path is user's home directory + "Project_YourCloud"
    dirs::home_dir()
        .map(|mut path| {
            path.push("Project_YourCloud");
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
        })
}

pub fn make_storage_directory(path: Option<PathBuf>) -> Result<PathBuf> {
    // Use provided path or generate default user storage folder
    let storage_path = match path {
        Some(p) => p,
        None => get_default_storage_path(),
    };

    info!("Storage directory created at path: {:?}", storage_path);
    Ok(storage_path)
}

pub async fn save_config(config: &Config) -> Result<()> {
    let config_path = get_config_path()?;

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
        pub max_storage_gib: f64,
        pub ws_url: String,
    }

    let save_config = SaveConfig {
        node_id: config.node_id.clone(),
        auth_token: config.auth_token.clone(),
        storage_path: config.storage_path.clone(),
        max_storage_gib: config.max_storage_gib,
        ws_url: config.ws_url.clone(),
    };

    debug!("Serializing config to TOML");
    let toml_string =
        toml::to_string_pretty(&save_config).context("Failed to serialize config to TOML")?;

    debug!("Writing config file ({} bytes)", toml_string.len());
    fs::write(&config_path, toml_string)
        .await
        .context("Failed to write config file")?;

    Ok(())
}

pub fn get_config_path() -> Result<PathBuf> {
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

pub fn validate_config(config: &Config) -> Result<()> {
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

    // Config storage misconfigured - add upper bound check
    if config.max_storage_gib == 0.0 {
        error!(
            "Invalid max_storage_gib: must be > 0, got: {}",
            config.max_storage_gib
        );
        return Err(anyhow::anyhow!("max_storage_gib must be greater than 0"));
    }

    // Check for excessively large storage values that could cause overflow
    const MAX_REASONABLE_GIB: f64 = 1024.0 * 1024.0;
    if config.max_storage_gib > MAX_REASONABLE_GIB {
        error!(
            "Invalid max_storage_gib: too large, got: {} GiB (max: {} GiB)",
            config.max_storage_gib, MAX_REASONABLE_GIB
        );
        return Err(anyhow::anyhow!(
            "max_storage_gib is too large: {} GiB (maximum allowed: {} GiB)",
            config.max_storage_gib,
            MAX_REASONABLE_GIB
        ));
    }

    // Validate the storage path doesn't contain dangerous characters
    let path_str = config.storage_path.to_string_lossy();
    if path_str.contains("..") {
        error!(
            "Storage path contains path traversal: {:?}",
            config.storage_path
        );
        return Err(anyhow::anyhow!(
            "storage_path contains invalid path traversal sequences"
        ));
    }

    debug!(
        "Max storage validation passed: {} GiB",
        config.max_storage_gib
    );

    // Validate node_id and auth_token if they're set
    if !config.node_id.is_empty() {
        if config.node_id.len() > 255 {
            return Err(anyhow::anyhow!("node_id is too long (max 255 characters)"));
        }
        // Basic validation for node_id format
        if !config
            .node_id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(anyhow::anyhow!("node_id contains invalid characters (only alphanumeric, hyphens, and underscores allowed)"));
        }
    }

    if !config.auth_token.is_empty() {
        if config.auth_token.len() > 1024 {
            return Err(anyhow::anyhow!(
                "auth_token is too long (max 1024 characters)"
            ));
        }
    }

    debug!("Configuration validation completed successfully");
    Ok(())
}
