use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use tracing::debug;

use crate::exporters::loki::LogBatcherHandle;
use crate::processors::process_logs;

use super::VercelSignatureVerifier;

/// State required by the Vercel log handler
pub struct VercelHandlerState {
    pub vercel_verifier: VercelSignatureVerifier,
    pub loki_handle: Option<LogBatcherHandle>,
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
    debug!(payload = %String::from_utf8_lossy(&body), "Received Vercel payload");

    // Spawn async processing (fast response)
    let loki_handle = state.loki_handle.clone();
    tokio::spawn(async move {
        process_logs(loki_handle, body).await;
    });

    StatusCode::OK.into_response()
}
