// src/webrtc.rs

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use crate::storage::{self, StorageState};

// Represents a temporary session for a chunk upload.
#[derive(Debug, Clone)]
pub struct TransferSession {
    pub chunk_id: String,
    pub buffer: Vec<u8>,
}

// Defines the message protocol used over the WebRTC data channel.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ClientMessage {
    // Messages received from client
    Upload { chunk_id: String },
    Download { chunk_id: String },
    DeleteChunk { chunk_id: String },
    CheckChunk { chunk_id: String },

    // Can be received (on upload) or sent
    TransferComplete { chunk_id: String },

    // Messages sent to client
    TransferError { chunk_id: String, error: String },
    ChunkStatus { chunk_id: String, exists: bool },
}

// Manages a single WebRTC peer connection, its state, and data transfers.
pub struct WebRTCManager {
    pub peer_connection: Arc<RTCPeerConnection>,
    // State needed for handling file operations, captured at creation time.
    storage_path: PathBuf,
    storage_state: Arc<StorageState>,
    // State for managing concurrent uploads.
    active_uploads: Arc<Mutex<HashMap<String, TransferSession>>>,
    current_upload_chunk_id: Arc<Mutex<Option<String>>>,
}

async fn wait_for_data_channel_buffer(data_channel: &Arc<RTCDataChannel>) {
    const HIGH_WATER_MARK: usize = 16 * 1024 * 1024; // 16MB
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
    pub async fn new(storage_path: PathBuf, storage_state: Arc<StorageState>) -> Result<Arc<Self>> {
        let pc = Arc::new(
            APIBuilder::new()
                .build()
                .new_peer_connection(RTCConfiguration::default())
                .await?,
        );

        let manager = Arc::new(Self {
            peer_connection: pc.clone(),
            storage_path,
            storage_state,
            active_uploads: Arc::new(Mutex::new(HashMap::new())),
            current_upload_chunk_id: Arc::new(Mutex::new(None)),
        });

        // Create a weak reference to self for use in the async callback.
        let manager_weak: Weak<WebRTCManager> = Arc::downgrade(&manager);

        // Define the handler for when the remote peer opens a data channel.
        pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            let manager_arc = match manager_weak.upgrade() {
                Some(arc) => arc,
                None => {
                    log::warn!(
                        "WebRTCManager instance dropped before data channel was established."
                    );
                    return Box::pin(async {});
                }
            };

            log::info!("New DataChannel '{}' with ID {}.", dc.label(), dc.id());

            // Define the message handler for this specific data channel.
            let dc_clone = dc.clone();
            dc.on_message(Box::new(move |msg: DataChannelMessage| {
                // Clone the Arc to move it into the async block.
                let manager_clone = manager_arc.clone();
                let dc_clone2 = dc_clone.clone();
                Box::pin(async move {
                    if let Err(e) = manager_clone.handle_message(msg, dc_clone2).await {
                        log::error!("Error handling data channel message: {}", e);
                    }
                })
            }));

            Box::pin(async {})
        }));

        Ok(manager)
    }

    // Handles an incoming SDP offer, creates an answer, and sets the local description.
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
            .ok_or_else(|| anyhow!("Local description not available after setting answer"))
    }

    // Deserializes and adds an ICE candidate received from the peer.
    pub async fn add_ice_candidate(&self, candidate_json: String) -> Result<()> {
        let candidate = serde_json::from_str::<
            webrtc::ice_transport::ice_candidate::RTCIceCandidateInit,
        >(&candidate_json)?;
        self.peer_connection.add_ice_candidate(candidate).await?;
        Ok(())
    }

    // Main message handler, called from the `on_message` callback.
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

    // Handles JSON control messages from the client.
    async fn handle_control_message(
        &self,
        data: &[u8],
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        let text = String::from_utf8_lossy(data);
        match serde_json::from_str::<ClientMessage>(&text) {
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
                    log::info!("Received DOWNLOAD request for chunk_id: {}", chunk_id);
                    let manager = self.clone_for_task();
                    tokio::spawn(async move {
                        if let Err(e) = manager
                            .handle_download_request(&chunk_id, data_channel)
                            .await
                        {
                            log::error!("Error during download for chunk {}: {}", chunk_id, e);
                        }
                    });
                }
                ClientMessage::DeleteChunk { chunk_id } => {
                    log::info!("Received DELETE request for chunk_id: {}", chunk_id);
                    let manager = self.clone_for_task();
                    tokio::spawn(async move {
                        if let Err(e) = manager.handle_delete_request(&chunk_id, data_channel).await
                        {
                            log::error!("Error during delete for chunk {}: {}", chunk_id, e);
                        }
                    });
                }
                ClientMessage::CheckChunk { chunk_id } => {
                    log::info!("Received CHECK request for chunk_id: {}", chunk_id);
                    let manager = self.clone_for_task();
                    tokio::spawn(async move {
                        if let Err(e) = manager.handle_check_request(&chunk_id, data_channel).await
                        {
                            log::error!("Error during check for chunk {}: {}", chunk_id, e);
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
                _ => log::warn!("Received unexpected client message type: {}", text),
            },
            Err(e) => log::warn!("Failed to parse control message: {}. Raw: '{}'", e, text),
        };
        Ok(())
    }

    // Handles incoming binary data for the currently active upload.
    async fn handle_data_chunk(&self, data: &[u8]) -> Result<()> {
        match &*self.current_upload_chunk_id.lock().await {
            Some(chunk_id) => {
                let mut uploads = self.active_uploads.lock().await;
                if let Some(session) = uploads.get_mut(chunk_id) {
                    session.buffer.extend_from_slice(data);
                } else {
                    log::error!("Received data for non-existent session: {}", chunk_id);
                }
            }
            _ => {
                log::warn!("Received unexpected binary data with no active upload.");
            }
        }
        Ok(())
    }

    async fn finalize_upload(
        &self,
        chunk_id: &str,
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        let mut uploads = self.active_uploads.lock().await;
        if let Some(session) = uploads.remove(chunk_id) {
            log::info!(
                "Finalizing upload for chunk {}. Total size: {} bytes",
                chunk_id,
                session.buffer.len()
            );
            match storage::store_chunk_data_to_disk(
                &self.storage_path,
                &session.chunk_id,
                &session.buffer,
                self.storage_state.clone(),
            )
            .await
            {
                Ok(_) => {
                    let msg = ClientMessage::TransferComplete {
                        chunk_id: chunk_id.to_string(),
                    };
                    data_channel.send_text(serde_json::to_string(&msg)?).await?;
                }
                Err(e) => {
                    self.send_error(chunk_id, &e.to_string(), &data_channel)
                        .await?;
                    return Err(e);
                }
            }
        } else {
            let err_msg = format!(
                "Cannot finalize: No upload session for chunk_id: {}",
                chunk_id
            );
            self.send_error(chunk_id, &err_msg, &data_channel).await?;
            return Err(anyhow!(err_msg));
        }
        Ok(())
    }

    async fn handle_download_request(
        &self,
        chunk_id: &str,
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        match storage::retrieve_chunk_data_from_disk(&self.storage_path, chunk_id).await {
            Ok(data) => {
                for chunk in data.chunks(16 * 1024) {
                    wait_for_data_channel_buffer(&data_channel).await;
                    data_channel.send(&Bytes::copy_from_slice(chunk)).await?;
                }
                let msg = ClientMessage::TransferComplete {
                    chunk_id: chunk_id.to_string(),
                };
                data_channel.send_text(serde_json::to_string(&msg)?).await?;
            }
            Err(e) => {
                self.send_error(chunk_id, &e.to_string(), &data_channel)
                    .await?
            }
        }
        Ok(())
    }

    async fn handle_delete_request(
        &self,
        chunk_id: &str,
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        match storage::delete_chunk_from_disk(
            &self.storage_path,
            chunk_id,
            self.storage_state.clone(),
        )
        .await
        {
            Ok(_) => {
                let msg = ClientMessage::TransferComplete {
                    chunk_id: chunk_id.to_string(),
                };
                data_channel.send_text(serde_json::to_string(&msg)?).await?;
            }
            Err(e) => {
                self.send_error(chunk_id, &e.to_string(), &data_channel)
                    .await?
            }
        }
        Ok(())
    }

    async fn handle_check_request(
        &self,
        chunk_id: &str,
        data_channel: Arc<RTCDataChannel>,
    ) -> Result<()> {
        match storage::check_chunk_exists(&self.storage_path, chunk_id).await {
            Ok(exists) => {
                let msg = ClientMessage::ChunkStatus {
                    chunk_id: chunk_id.to_string(),
                    exists,
                };
                data_channel.send_text(serde_json::to_string(&msg)?).await?;
            }
            Err(e) => {
                self.send_error(chunk_id, &e.to_string(), &data_channel)
                    .await?
            }
        }
        Ok(())
    }

    // Creates a clone of the necessary state for spawning a new async task.
    fn clone_for_task(&self) -> Self {
        Self {
            peer_connection: self.peer_connection.clone(),
            storage_path: self.storage_path.clone(),
            storage_state: self.storage_state.clone(),
            active_uploads: self.active_uploads.clone(),
            current_upload_chunk_id: self.current_upload_chunk_id.clone(),
        }
    }

    // Sends a formatted error message to the client.
    async fn send_error(
        &self,
        chunk_id: &str,
        error: &str,
        dc: &Arc<RTCDataChannel>,
    ) -> Result<()> {
        log::error!("Error for chunk {}: {}", chunk_id, error);
        let err_msg = ClientMessage::TransferError {
            chunk_id: chunk_id.to_string(),
            error: error.to_string(),
        };
        dc.send_text(serde_json::to_string(&err_msg)?).await?;
        Ok(())
    }
}
