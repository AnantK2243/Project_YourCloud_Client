// src/config.rs

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use anyhow::{Context, Result};
use dirs;
use tokio::fs;
use std::env; // Added for environment variable access

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub node_id: String,
    pub auth_token: String,
    pub backend_api_url: String,
    pub backend_ws_url: String,
    pub storage_path: PathBuf,
    pub max_storage_gib: u64,
    pub log_level: String,
    pub check_interval_seconds: u64,
    pub recalibration_interval_seconds: u64,
}

impl Default for Config {
    fn default() -> Self {
        let default_storage_path = dirs::home_dir()
            .map(|mut path| {
                path.push("Project_YourCloud");
                path
            })
            .or_else(|| {
                env::var("USER")
                    .ok()
                    .map(|user_name| PathBuf::from(format!("/home/{}/Project_YourCloud", user_name)))
            })
            .unwrap_or_else(|| {
                log::warn!("Could not determine home directory or USER env var. Defaulting storage_path to relative 'cloud_storage_node_data'.");
                PathBuf::from("cloud_storage_node_data") 
            });

        Self {
            node_id: String::new(),
            auth_token: String::new(),
            backend_api_url: "".to_string(),
            backend_ws_url: "".to_string(),
            storage_path: default_storage_path,
            max_storage_gib: 20,
            log_level: "info".to_string(),
            check_interval_seconds: 60,
            recalibration_interval_seconds: 3600,
        }
    }
}

pub async fn load_config() -> Result<Config> {
    let config_path =get_config_path()?;
    
    if !config_path.exists() {
        log::info!("Config file not found, creating default at: {:?}", config_path);
        let default_config = Config::default();
        save_config(&default_config).await?;
        return Ok(default_config);
    }

    log::info!("Config found: {:?}", config_path);

    let config_content = fs::read_to_string(&config_path).await
        .context("Failed to read config file")?;

    let config: Config = toml::from_str(&config_content)
        .context("Failed to parse config file")?;

    validate_config(&config)?;
    Ok(config)
}

pub async fn save_config(config: &Config) -> Result<()> {
    let config_path = get_config_path()?;
    
    // Create parent directories if they don't exist
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    
    let toml_string = toml::to_string_pretty(config)?;
    fs::write(&config_path, toml_string).await?;
    
    Ok(())
}

fn get_config_path() -> Result<PathBuf> {
    let config_path = dirs::config_dir()
        .map(|dir| dir.join("Project_YourCloud").join("config.toml"))
        .unwrap_or_else(|| PathBuf::from("/etc/Project_YourCloud/config.toml"));
    
    Ok(config_path)
}

fn validate_config(config: &Config) -> Result<()> {
    if !config.storage_path.is_absolute() {
        return Err(anyhow::anyhow!("storage_path must be an absolute path. Current: {:?}", config.storage_path));
    }
    if config.max_storage_gib <= 0 {
        return Err(anyhow::anyhow!("max_storage_gib must be greater than 0"));
    }
    Ok(())
}