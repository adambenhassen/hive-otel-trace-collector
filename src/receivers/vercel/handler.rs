use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use crate::exporters::loki::LogBatcherHandle;
use crate::processors::process_logs;

use super::VercelSignatureVerifier;

/// State required by the Vercel log handler
pub struct VercelHandlerState {
    pub vercel_verifier: VercelSignatureVerifier,
    pub log_batcher_handle: LogBatcherHandle,
}

pub async fn vercel_log_handler(
    State(state): State<Arc<VercelHandlerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Verify Vercel signature
    let signature = match headers.get("x-vercel-signature") {
        Some(sig) => match sig.to_str() {
            Ok(s) => s,
            Err(_) => {
                return (StatusCode::BAD_REQUEST, "Invalid signature header").into_response()
            }
        },
        None => return (StatusCode::UNAUTHORIZED, "Missing X-Vercel-Signature").into_response(),
    };

    if !state.vercel_verifier.verify(signature, &body) {
        return (StatusCode::UNAUTHORIZED, "Invalid signature").into_response();
    }

    // Extract target_id from X-Hive-Target-Ref header
    let target_id = match headers.get("x-hive-target-ref") {
        Some(id) => match id.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return (StatusCode::BAD_REQUEST, "Invalid target ref header").into_response(),
        },
        None => return (StatusCode::BAD_REQUEST, "Missing X-Hive-Target-Ref").into_response(),
    };

    // Spawn async processing (fast response)
    let log_handle_clone = state.log_batcher_handle.clone();
    tokio::spawn(async move {
        process_logs(log_handle_clone, body, target_id).await;
    });

    StatusCode::OK.into_response()
}
