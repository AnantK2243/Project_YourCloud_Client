// src/webrtc.rs

use crate::storage;
use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use serde_json;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::protocol::Message;
use webrtc::{
    api::{
        interceptor_registry::register_default_interceptors, media_engine::MediaEngine, APIBuilder,
    },
    data_channel::{data_channel_message::DataChannelMessage, RTCDataChannel},
    ice_transport::ice_candidate::RTCIceCandidateInit,
    ice_transport::ice_server::RTCIceServer,
    interceptor::registry::Registry,
    peer_connection::{
        configuration::RTCConfiguration, 
        peer_connection_state::RTCPeerConnectionState,
        policy::{
            bundle_policy::RTCBundlePolicy,
            ice_transport_policy::RTCIceTransportPolicy,
            rtcp_mux_policy::RTCRtcpMuxPolicy,
        },
        sdp::session_description::RTCSessionDescription, 
        RTCPeerConnection,
    },
};

// Global session manager
static WEBRTC_MANAGER: Lazy<WebRTCManager> = Lazy::new(|| WebRTCManager::new());

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WebRTCSession {
    pub session_id: String,
    pub user_id: String,
    pub peer_connection: Arc<RTCPeerConnection>,
    pub file_metadata: serde_json::Value,
    pub created_at: std::time::Instant,
    pub pending_chunk_id: Arc<Mutex<Option<String>>>,
}

pub struct WebRTCManager {
    sessions: Arc<Mutex<HashMap<String, WebRTCSession>>>,
}

impl WebRTCManager {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn create_session(
        &self,
        session_id: String,
        user_id: String,
        file_metadata: serde_json::Value,
        storage_path: PathBuf,
        current_used_space_bytes: Arc<AtomicU64>,
        max_storage_bytes: Arc<AtomicU64>,
        current_chunk_count: Arc<AtomicU64>,
        response_tx: mpsc::Sender<Message>,
    ) -> Result<()> {
        info!("Creating WebRTC session: {}", session_id);

        // Create MediaEngine and register codecs
        let mut m = MediaEngine::default();

        // Create a InterceptorRegistry
        let mut registry = Registry::new();
        registry = register_default_interceptors(registry, &mut m)?;

        // Create the API object
        let api = APIBuilder::new()
            .with_media_engine(m)
            .with_interceptor_registry(registry)
            .build();

        // Create ICE configuration
        let config = RTCConfiguration {
            ice_servers: vec![RTCIceServer {
                urls: vec!["stun:172.31.127.22:3478".to_string()],
                username: String::new(),
                credential: String::new(),
                ..Default::default()
            }],
            ice_transport_policy: RTCIceTransportPolicy::All,
            bundle_policy: RTCBundlePolicy::Balanced,
            rtcp_mux_policy: RTCRtcpMuxPolicy::Require,
            ..Default::default()
        };

        // Create peer connection
        let peer_connection = Arc::new(api.new_peer_connection(config).await?);

        // Set up connection state handler
        let _pc_clone = Arc::clone(&peer_connection);
        let session_id_clone = session_id.clone();
        peer_connection.on_peer_connection_state_change(Box::new(move |s| {
            let session_id = session_id_clone.clone();
            Box::pin(async move {
                info!(
                    "WebRTC connection state for session {}: {:?}",
                    session_id, s
                );
                if s == RTCPeerConnectionState::Failed {
                    error!("WebRTC connection failed for session: {}", session_id);
                }
            })
        }));

        // Set up ICE connection state handler
        let session_id_ice = session_id.clone();
        peer_connection.on_ice_connection_state_change(Box::new(move |s| {
            let session_id = session_id_ice.clone();
            Box::pin(async move {
                info!(
                    "ICE connection state for session {}: {:?}",
                    session_id, s
                );
            })
        }));

        // Set up data channel handler for incoming chunks
        let storage_path_clone = storage_path.clone();
        let used_space_clone = Arc::clone(&current_used_space_bytes);
        let max_storage_clone = Arc::clone(&max_storage_bytes);
        let chunk_count_clone = Arc::clone(&current_chunk_count);
        let session_id_data = session_id.clone();

        peer_connection.on_data_channel(Box::new(move |d| {
            let storage_path = storage_path_clone.clone();
            let used_space = Arc::clone(&used_space_clone);
            let max_storage = Arc::clone(&max_storage_clone);
            let chunk_count = Arc::clone(&chunk_count_clone);
            let session_id = session_id_data.clone();

            Box::pin(async move {
                info!("Data channel opened for session: {}", session_id);

                let d_clone = Arc::clone(&d);
                d.on_message(Box::new(move |msg| {
                    let storage_path = storage_path.clone();
                    let used_space = Arc::clone(&used_space);
                    let max_storage = Arc::clone(&max_storage);
                    let chunk_count = Arc::clone(&chunk_count);
                    let session_id = session_id.clone();
                    let data_channel = Arc::clone(&d_clone);

                    Box::pin(async move {
                        if let Err(e) = handle_data_channel_message(
                            msg,
                            &storage_path,
                            &used_space,
                            &max_storage,
                            &chunk_count,
                            &session_id,
                            &data_channel,
                        )
                        .await
                        {
                            error!(
                                "Error handling data channel message for session {}: {}",
                                session_id, e
                            );
                        }
                    })
                }));
            })
        }));

        // Set up ICE candidate handler
        let response_tx_ice = response_tx.clone();
        let session_id_ice = session_id.clone();
        peer_connection.on_ice_candidate(Box::new(move |c| {
            let response_tx = response_tx_ice.clone();
            let session_id = session_id_ice.clone();
            Box::pin(async move {
                if let Some(candidate) = c {
                    let ice_message = serde_json::json!({
                        "type": "P2P_RELAY",
                        "sessionId": session_id,
                        "payload": {
                            "type": "ice-candidate",
                            "candidate": candidate.to_json().unwrap_or_default()
                        }
                    });

                    if let Err(e) = response_tx
                        .send(Message::Text(ice_message.to_string()))
                        .await
                    {
                        error!(
                            "Failed to send ICE candidate for session {}: {}",
                            session_id, e
                        );
                    } else {
                        debug!("Sent ICE candidate for session {}", session_id);
                    }
                }
            })
        }));

        // Store session
        let session = WebRTCSession {
            session_id: session_id.clone(),
            user_id,
            peer_connection: Arc::clone(&peer_connection),
            file_metadata,
            created_at: std::time::Instant::now(),
            pending_chunk_id: Arc::new(Mutex::new(None)),
        };

        {
            let mut sessions = self.sessions.lock().await;
            sessions.insert(session_id.clone(), session);
        }

        info!("WebRTC session created successfully: {}", session_id);
        Ok(())
    }

    pub async fn get_session(&self, session_id: &str) -> Option<WebRTCSession> {
        let sessions = self.sessions.lock().await;
        sessions.get(session_id).cloned()
    }

    pub async fn remove_session(&self, session_id: &str) -> Option<WebRTCSession> {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.remove(session_id) {
            info!("Removed WebRTC session: {}", session_id);

            // Close peer connection
            if let Err(e) = session.peer_connection.close().await {
                error!(
                    "Error closing peer connection for session {}: {}",
                    session_id, e
                );
            }

            Some(session)
        } else {
            None
        }
    }

    pub async fn process_offer(
        &self,
        session_id: &str,
        offer_sdp: &str,
        response_tx: &mpsc::Sender<Message>,
    ) -> Result<()> {
        let session = self
            .get_session(session_id)
            .await
            .ok_or_else(|| anyhow!("Session not found: {}", session_id))?;

        // Set remote description (offer)
        let offer = RTCSessionDescription::offer(offer_sdp.to_string())?;
        session
            .peer_connection
            .set_remote_description(offer)
            .await?;

        // Create answer
        let answer = session.peer_connection.create_answer(None).await?;
        session
            .peer_connection
            .set_local_description(answer.clone())
            .await?;

        // Send answer back via WebSocket signaling
        let answer_message = serde_json::json!({
            "type": "P2P_RELAY",
            "sessionId": session_id,
            "payload": {
                "type": "answer",
                "sdp": answer.sdp
            }
        });

        response_tx
            .send(Message::Text(answer_message.to_string()))
            .await?;
        info!("Sent WebRTC answer for session: {}", session_id);

        Ok(())
    }

    pub async fn add_ice_candidate(
        &self,
        session_id: &str,
        candidate_json: &serde_json::Value,
    ) -> Result<()> {
        let session = self
            .get_session(session_id)
            .await
            .ok_or_else(|| anyhow!("Session not found: {}", session_id))?;

        // Parse the ICE candidate JSON into RTCIceCandidateInit
        if let Ok(candidate_str) = serde_json::to_string(candidate_json) {
            if let Ok(candidate_init) = serde_json::from_str::<RTCIceCandidateInit>(&candidate_str)
            {
                session
                    .peer_connection
                    .add_ice_candidate(candidate_init)
                    .await?;
                debug!("Added ICE candidate for session: {}", session_id);
            } else {
                warn!("Failed to parse ICE candidate for session: {}", session_id);
            }
        } else {
            warn!("Invalid ICE candidate JSON for session: {}", session_id);
        }

        Ok(())
    }
}

// Handle incoming data channel messages
async fn handle_data_channel_message(
    msg: DataChannelMessage,
    storage_path: &PathBuf,
    current_used_space_bytes: &Arc<AtomicU64>,
    max_storage_bytes: &Arc<AtomicU64>,
    current_chunk_count: &Arc<AtomicU64>,
    session_id: &str,
    data_channel: &Arc<RTCDataChannel>,
) -> Result<()> {
    let data = msg.data.to_vec();

    // Check if this is a text message or binary data
    if let Ok(text) = String::from_utf8(data.clone()) {
        // Try to parse as JSON chunk metadata
        if let Ok(metadata) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(chunk_id) = metadata.get("chunk_id").and_then(|v| v.as_str()) {
                // This is chunk metadata - store it for the next binary message
                if let Some(session) = WEBRTC_MANAGER.get_session(session_id).await {
                    let mut pending = session.pending_chunk_id.lock().await;
                    *pending = Some(chunk_id.to_string());
                    debug!(
                        "Received chunk metadata via WebRTC: {} for session: {}",
                        chunk_id, session_id
                    );

                    // Send acknowledgment
                    if let Err(e) = data_channel.send_text("READY".to_string()).await {
                        error!("Failed to send ready response: {}", e);
                    }
                    return Ok(());
                }
            }
        }
    }

    // This is binary data - get the pending chunk ID
    let chunk_id = if let Some(session) = WEBRTC_MANAGER.get_session(session_id).await {
        let mut pending = session.pending_chunk_id.lock().await;
        if let Some(id) = pending.take() {
            id
        } else {
            // No pending chunk ID - error out as fallback
            error!(
                "Received binary data without pending chunk ID for session: {}",
                session_id
            );
            let response = "ERROR:No chunk ID provided for binary data";
            if let Err(e) = data_channel.send_text(response.to_string()).await {
                error!("Failed to send error response: {}", e);
            }
            return Err(anyhow!("Binary data received without pending chunk ID"));
        }
    } else {
        // No session - error out
        error!("Received binary data for unknown session: {}", session_id);
        let response = "ERROR:Unknown session";
        if let Err(e) = data_channel.send_text(response.to_string()).await {
            error!("Failed to send error response: {}", e);
        }
        return Err(anyhow!("Binary data received for unknown session"));
    };

    debug!(
        "Storing encrypted chunk via WebRTC: {} ({} bytes) for session: {}",
        chunk_id,
        data.len(),
        session_id
    );

    match storage::store_chunk_data_to_disk(
        storage_path,
        &chunk_id,
        &data,
        current_used_space_bytes,
        max_storage_bytes,
        current_chunk_count,
    )
    .await
    {
        Ok(chunk_size) => {
            info!(
                "Successfully stored chunk {} ({} bytes) via WebRTC",
                chunk_id, chunk_size
            );

            // Send simple text response with chunk ID
            let response = format!("STORED:{}", chunk_id);
            if let Err(e) = data_channel.send_text(response).await {
                error!(
                    "Failed to send success response for chunk {}: {}",
                    chunk_id, e
                );
            }
        }
        Err(e) => {
            error!("Failed to store chunk {} via WebRTC: {}", chunk_id, e);

            // Send simple error response
            let response = format!("ERROR:{}", e);
            if let Err(send_err) = data_channel.send_text(response).await {
                error!(
                    "Failed to send error response for chunk {}: {}",
                    chunk_id, send_err
                );
            }

            return Err(e);
        }
    }

    Ok(())
}

// Public API functions for use in commands.rs
pub async fn create_webrtc_session(
    session_id: String,
    user_id: String,
    file_metadata: serde_json::Value,
    storage_path: PathBuf,
    current_used_space_bytes: Arc<AtomicU64>,
    max_storage_bytes: Arc<AtomicU64>,
    current_chunk_count: Arc<AtomicU64>,
    response_tx: mpsc::Sender<Message>,
) -> Result<()> {
    WEBRTC_MANAGER
        .create_session(
            session_id,
            user_id,
            file_metadata,
            storage_path,
            current_used_space_bytes,
            max_storage_bytes,
            current_chunk_count,
            response_tx,
        )
        .await
}

pub async fn process_webrtc_offer(
    session_id: &str,
    offer_sdp: &str,
    response_tx: &mpsc::Sender<Message>,
) -> Result<()> {
    WEBRTC_MANAGER
        .process_offer(session_id, offer_sdp, response_tx)
        .await
}

pub async fn add_webrtc_ice_candidate(
    session_id: &str,
    candidate_json: &serde_json::Value,
) -> Result<()> {
    WEBRTC_MANAGER
        .add_ice_candidate(session_id, candidate_json)
        .await
}

pub async fn close_webrtc_session(session_id: &str) -> Option<WebRTCSession> {
    WEBRTC_MANAGER.remove_session(session_id).await
}
