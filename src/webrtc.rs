// src/webrtc.rs

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::protocol::Message;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::RTCIceCandidate;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use crate::storage::{self, StorageState};

/// Represents a temporary session for a chunk upload.
#[derive(Debug, Clone)]
pub struct TransferSession {
    pub chunk_id: String,
    pub buffer: Vec<u8>,
}

/// Defines the message protocol used over the WebRTC data channel.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ClientMessage {
    Upload { chunk_id: String },
    Download { chunk_id: String },
    Delete { chunk_id: String },
    CheckId { chunk_id: String },
    TransferComplete { chunk_id: String },
    TransferError { chunk_id: String, error: String },
    IdStatus { chunk_id: String, exists: bool },
}

/// Manages a single WebRTC peer connection, its state, and data transfers.
pub struct WebRTCManager {
    pub peer_connection: Arc<RTCPeerConnection>,
    storage_path: PathBuf,
    storage_state: Arc<StorageState>,
    active_uploads: Arc<Mutex<HashMap<String, TransferSession>>>,
    current_upload_chunk_id: Arc<Mutex<Option<String>>>,
}

async fn wait_for_data_channel_buffer(data_channel: &Arc<RTCDataChannel>) {
    const HIGH_WATER_MARK: usize = 16 * 1024 * 1024;
    while data_channel.buffered_amount().await > HIGH_WATER_MARK {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut tx = Some(tx);
        data_channel
            .on_buffered_amount_low(Box::new(move || {
                if let Some(tx) = tx.take() {
                    let _ = tx.send(());
                }
                Box::pin(async {})
            }))
            .await;
        let _ = rx.await;
    }
}

impl WebRTCManager {
    /// Creates a new `WebRTCManager` and sets up all necessary WebRTC event handlers.
    pub async fn new(
        storage_path: PathBuf,
        storage_state: Arc<StorageState>,
        response_tx: mpsc::Sender<Message>,
        session_id: String,
    ) -> Result<Arc<Self>> {
        let mut setting_engine = SettingEngine::default();
        setting_engine.set_network_types(vec![webrtc::ice::network_type::NetworkType::Udp4]);

        let api = APIBuilder::new().with_setting_engine(setting_engine).build();

        let config = RTCConfiguration {
            ice_servers: vec![
                RTCIceServer {
                    urls: vec![
                        "stun:stun.l.google.com:19302".to_owned(),
                        "stun:global.stun.twilio.com:3478".to_owned(),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        
        let pc = Arc::new(api.new_peer_connection(config).await?);

        let manager = Arc::new(Self {
            peer_connection: pc.clone(),
            storage_path,
            storage_state,
            active_uploads: Arc::new(Mutex::new(HashMap::new())),
            current_upload_chunk_id: Arc::new(Mutex::new(None)),
        });

        pc.on_ice_candidate(Box::new(move |candidate: Option<RTCIceCandidate>| {
            let response_tx = response_tx.clone();
            let session_id = session_id.clone();
            Box::pin(async move {
                if let Some(candidate) = candidate {
                    match candidate.to_json() {
                        Ok(candidate_json) => {
                            let payload = serde_json::json!({
                                "command_id": session_id,
                                "type": "ICE_CANDIDATE_ANSWER",
                                "candidate": candidate_json,
                            });
                            let msg = Message::Text(payload.to_string());
                            if response_tx.send(msg).await.is_err() {
                                log::error!("Failed to send ICE candidate: response channel closed.");
                            }
                        }
                        Err(e) => {
                            log::error!("Failed to serialize ICE candidate to JSON: {}", e);
                        }
                    }
                }
            })
        }));

        let manager_weak: Weak<WebRTCManager> = Arc::downgrade(&manager);

        pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            let manager_arc = match manager_weak.upgrade() {
                Some(arc) => arc,
                None => {
                    return Box::pin(async {
                        log::warn!("WebRTCManager dropped before data channel established.")
                    })
                }
            };
            log::info!("New DataChannel '{}' with ID {}.", dc.label(), dc.id());

            let dc_clone = dc.clone();
            dc.on_message(Box::new(move |msg: DataChannelMessage| {
                let manager_clone = manager_arc.clone();
                let dc_clone = dc_clone.clone();
                Box::pin(async move {
                    if let Err(e) = manager_clone.handle_message(msg, dc_clone).await {
                        log::error!("Error handling data channel message: {}", e);
                    }
                })
            }));
            Box::pin(async {})
        }));
        Ok(manager)
    }

    pub async fn handle_offer_and_create_answer(
        &self,
        offer: RTCSessionDescription,
    ) -> Result<RTCSessionDescription> {
        self.peer_connection.set_remote_description(offer).await?;
        let answer = self.peer_connection.create_answer(None).await?;
        self.peer_connection.set_local_description(answer).await?;
        self.peer_connection
            .local_description()
            .await
            .ok_or_else(|| anyhow!("Local description not available"))
    }

    pub async fn add_ice_candidate(&self, candidate_json: String) -> Result<()> {
        let candidate =
            serde_json::from_str::<webrtc::ice_transport::ice_candidate::RTCIceCandidateInit>(
                &candidate_json,
            )?;
        self.peer_connection.add_ice_candidate(candidate).await?;
        Ok(())
    }

    async fn handle_message(
        &self,
        msg: DataChannelMessage,
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        if msg.is_string {
            self.handle_control_message(&msg.data, data_channel).await?;
        } else {
            self.handle_data_chunk(&msg.data).await?;
        }
        Ok(())
    }

    async fn handle_control_message(
        &self,
        data: &[u8],
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        match serde_json::from_slice::<ClientMessage>(data) {
            Ok(message) => match message {
                ClientMessage::Upload { chunk_id } => {
                    log::info!("Received UPLOAD request for chunk_id: {}", chunk_id);
                    let session = TransferSession {
                        chunk_id: chunk_id.clone(),
                        buffer: Vec::new(),
                    };
                    self.active_uploads
                        .lock()
                        .await
                        .insert(chunk_id.clone(), session);
                    *self.current_upload_chunk_id.lock().await = Some(chunk_id);
                }
                ClientMessage::Download { chunk_id } => {
                    let manager_clone = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            manager_clone.handle_download_request(&chunk_id, data_channel).await
                        {
                            log::error!("Error in download task for chunk {}: {}", chunk_id, e);
                        }
                    });
                }
                ClientMessage::Delete { chunk_id } => {
                    let manager_clone = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            manager_clone.handle_delete_request(&chunk_id, data_channel).await
                        {
                            log::error!("Error in delete task for chunk {}: {}", chunk_id, e);
                        }
                    });
                }
                ClientMessage::CheckId { chunk_id } => {
                    let manager_clone = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            manager_clone.handle_check_request(&chunk_id, data_channel).await
                        {
                            log::error!("Error in check task for chunk {}: {}", chunk_id, e);
                        }
                    });
                }
                ClientMessage::TransferComplete { chunk_id } => {
                    log::info!(
                        "Received TRANSFER_COMPLETE for upload of chunk_id: {}",
                        chunk_id
                    );
                    self.finalize_upload(&chunk_id, data_channel).await?;
                    *self.current_upload_chunk_id.lock().await = None;
                }
                _ => log::warn!("Received unexpected client message type"),
            },
            Err(e) => log::warn!("Failed to parse control message: {}", e),
        };
        Ok(())
    }

    async fn handle_data_chunk(&self, data: &[u8]) -> Result<()> {
        if let Some(chunk_id) = &*self.current_upload_chunk_id.lock().await {
            if let Some(session) = self.active_uploads.lock().await.get_mut(chunk_id) {
                session.buffer.extend_from_slice(data);
            }
        }
        Ok(())
    }

    async fn finalize_upload(
        &self,
        chunk_id: &str,
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        if let Some(session) = self.active_uploads.lock().await.remove(chunk_id) {
            storage::store_chunk_data_to_disk(
                &self.storage_path,
                &session.chunk_id,
                &session.buffer,
                self.storage_state.clone(),
            )
            .await?;
            let msg = ClientMessage::TransferComplete {
                chunk_id: chunk_id.to_string(),
            };
            data_channel
                .send_text(serde_json::to_string(&msg)?)
                .await?;
        } else {
            return Err(anyhow!(
                "Cannot finalize: No upload session for chunk_id: {}",
                chunk_id
            ));
        }
        Ok(())
    }

    async fn handle_download_request(
        &self,
        chunk_id: &str,
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        let data = storage::retrieve_chunk_data_from_disk(
            &self.storage_path,
            chunk_id,
        )
        .await?;
        for chunk in data.chunks(16 * 1024) {
            wait_for_data_channel_buffer(&data_channel).await;
            data_channel
                .send(&Bytes::copy_from_slice(chunk))
                .await?;
        }
        let msg = ClientMessage::TransferComplete {
            chunk_id: chunk_id.to_string(),
        };
        data_channel
            .send_text(serde_json::to_string(&msg)?)
            .await?;
        Ok(())
    }

    async fn handle_delete_request(
        &self,
        chunk_id: &str,
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        storage::delete_chunk_from_disk(
            &self.storage_path,
            chunk_id,
            self.storage_state.clone(),
        )
        .await?;
        let msg = ClientMessage::TransferComplete {
            chunk_id: chunk_id.to_string(),
        };
        data_channel
            .send_text(serde_json::to_string(&msg)?)
            .await?;
        Ok(())
    }

    async fn handle_check_request(
        &self,
        chunk_id: &str,
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        let exists = storage::check_chunk_exists(&self.storage_path, chunk_id).await?;
        let msg = ClientMessage::IdStatus {
            chunk_id: chunk_id.to_string(),
            exists,
        };
        data_channel
            .send_text(serde_json::to_string(&msg)?)
            .await?;
        Ok(())
    }
}

impl Clone for WebRTCManager {
    fn clone(&self) -> Self {
        Self {
            peer_connection: self.peer_connection.clone(),
            storage_path: self.storage_path.clone(),
            storage_state: self.storage_state.clone(),
            active_uploads: self.active_uploads.clone(),
            current_upload_chunk_id: self.current_upload_chunk_id.clone(),
        }
    }
}