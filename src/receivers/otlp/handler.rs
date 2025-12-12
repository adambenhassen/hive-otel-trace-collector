use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode},
    response::{IntoResponse, Response},
};
use tracing::{debug, warn};

use crate::buffers::disk::MmapRingBuffer;
use crate::exporters::clickhouse::BatcherHandle;
use crate::processors::{TraceProcessorConfig, process_traces};

use super::{extract_and_validate, Authenticator};

const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// State required by the trace handler
pub struct TraceHandlerState {
    pub authenticator: Authenticator,
    pub batcher_handle: BatcherHandle,
    pub disk_buffer: Option<Arc<MmapRingBuffer>>,
    pub mem_buffer_limit_mb: u64,
    pub direct_to_disk_threshold_bytes: usize,
    pub in_backpressure: Arc<AtomicBool>,
}

pub async fn trace_handler(
    State(state): State<Arc<TraceHandlerState>>,
    req: Request<Body>,
) -> Response {
    // Check if under memory backpressure
    if state.in_backpressure.load(Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Server under memory backpressure").into_response();
    }

    let headers = req.headers().clone();

    // Detect content type
    let is_json = match headers.get(header::CONTENT_TYPE) {
        Some(ct) => match ct.to_str() {
            Ok(s) => s.contains("application/json"),
            Err(_) => {
                debug!("Content-Type header is not valid UTF-8, treating as protobuf");
                false
            }
        },
        None => {
            debug!("No Content-Type header, defaulting to protobuf");
            false
        }
    };

    // Extract and validate auth
    let target_id = match extract_and_validate(&state.authenticator, &headers).await {
        Ok(id) => id,
        Err(response) => return response,
    };

    // Read request body with timeout
    let body_result = tokio::time::timeout(
        BODY_READ_TIMEOUT,
        axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024),
    )
    .await;

    let body_bytes = match body_result {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => {
            return (StatusCode::BAD_REQUEST, format!("Failed to read body: {}", e)).into_response();
        }
        Err(_) => {
            warn!("Request body read timed out after {:?}", BODY_READ_TIMEOUT);
            return (StatusCode::REQUEST_TIMEOUT, "Request body read timeout").into_response();
        }
    };

    let processor_config = TraceProcessorConfig {
        disk_buffer: state.disk_buffer.clone(),
        batcher_handle: state.batcher_handle.clone(),
        mem_buffer_limit_mb: state.mem_buffer_limit_mb,
        direct_to_disk_threshold_bytes: state.direct_to_disk_threshold_bytes,
    };

    tokio::spawn(async move {
        process_traces(&processor_config, body_bytes, is_json, target_id).await;
    });
    StatusCode::OK.into_response()
}
