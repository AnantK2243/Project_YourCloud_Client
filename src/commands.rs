// src/commands.rs

use crate::network;
use crate::storage::StorageState;
use crate::webrtc::WebRTCManager;
use anyhow::{anyhow, Result};
use log::{error, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::protocol::Message;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

pub type WebRTCConnections = Arc<Mutex<HashMap<String, Arc<WebRTCManager>>>>;

/// Defines the commands that the Rust client can receive from the backend proxy server.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "command_type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BackendCommand {
    WebRtcOffer {
        command_id: String,
        offer: serde_json::Value,
    },
    WebRtcIceCandidate {
        command_id: String,
        candidate: String,
    },
    StatusRequest {
        command_id: String,
    },
    #[serde(other)]
    Unknown,
}

pub async fn handle_command(
    command: BackendCommand,
    response_tx: mpsc::Sender<Message>,
    storage_state: Arc<StorageState>,
    webrtc_connections: WebRTCConnections,
    storage_path: PathBuf,
) -> Result<()> {
    match command {
        BackendCommand::WebRtcOffer { command_id, offer } => {
            if webrtc_connections.lock().await.contains_key(&command_id) {
                warn!(
                    "Duplicate WebRTC offer for session: {}. Ignoring.",
                    command_id
                );
                return Ok(());
            }

            // Create a new WebRTCManager, passing the necessary state.
            let manager = WebRTCManager::new(storage_path, storage_state).await?;

            // Insert the new manager Arc into our shared state map.
            webrtc_connections
                .lock()
                .await
                .insert(command_id.clone(), manager.clone());

            let offer_sdp: RTCSessionDescription = serde_json::from_value(offer)
                .map_err(|e| anyhow!("Failed to deserialize offer SDP: {}", e))?;

            match manager.handle_offer_and_create_answer(offer_sdp).await {
                Ok(answer) => {
                    let answer_payload = serde_json::json!({
                        "command_id": command_id,
                        "type": "WEB_RTC_ANSWER",
                        "answer": answer,
                    });
                    response_tx
                        .send(Message::Text(answer_payload.to_string()))
                        .await?;
                }
                Err(e) => {
                    error!(
                        "Failed to handle WebRTC offer for session {}: {}",
                        command_id, e
                    );
                    webrtc_connections.lock().await.remove(&command_id); // Cleanup on failure
                    return Err(e);
                }
            }
        }

        BackendCommand::WebRtcIceCandidate {
            command_id,
            candidate,
        } => {
            if let Some(manager) = webrtc_connections.lock().await.get(&command_id) {
                if let Err(e) = manager.add_ice_candidate(candidate).await {
                    error!(
                        "Failed to add ICE candidate for session {}: {}",
                        command_id, e
                    );
                }

                // Reply with acknowledgment
                let ack_payload = serde_json::json!({
                    "command_id": command_id,
                    "type": "ICE_CANDIDATE_ACK",
                    "status": "received",
                });

                response_tx
                    .send(Message::Text(ack_payload.to_string()))
                    .await?;
            } else {
                warn!("Received ICE candidate for unknown session: {}", command_id);
            }
        }

        BackendCommand::StatusRequest { command_id } => {
            network::send_status_report(&response_tx, command_id, storage_state).await?;
        }

        BackendCommand::Unknown => {
            warn!("Received unknown or unparsable command - ignoring.");
        }
    }

    Ok(())
}
