#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod auth;
mod diskbuffer;
mod metrics;
mod pipeline;
mod proto;

use auth::Authenticator;
use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};
use diskbuffer::{BufferConfig, BufferedBatch, MmapRingBuffer};
use pipeline::{Batcher, BatcherConfig, BatcherError, BatcherHandle, ClickHouseConfig, InsertPool};
use reqwest::Client;
use std::{env, sync::Arc, time::Duration};
use tokio::signal;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn, Level};
use url::Url;

const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30); // Request body read timeout

// Threshold for writing directly to disk (skip channel to avoid OOM)
const DIRECT_TO_DISK_THRESHOLD_SPANS: usize = 1000;
const DIRECT_TO_DISK_THRESHOLD_BYTES: usize = 1024 * 1024; // 1MB

struct AppState {
    authenticator: Authenticator,
    batcher_handle: BatcherHandle,
    disk_buffer: Arc<MmapRingBuffer>,
    disk_buffer_enabled: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    let auth_endpoint = env::var("HIVE_OTEL_AUTH_ENDPOINT")
        .unwrap_or_else(|_| "http://graphql-api:4000/otel-auth".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "4318".to_string());
    let addr = format!("0.0.0.0:{}", port);

    // Validate URLs at startup
    Url::parse(&auth_endpoint).expect("AUTH_ENDPOINT is not a valid URL");

    info!("Starting OTLP collector on {}", addr);
    info!("Auth endpoint: {}", auth_endpoint);

    let http_client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client");

    let authenticator = Authenticator::new(http_client, auth_endpoint);

    let ch_config = ClickHouseConfig::from_env();
    let buffer_config = BufferConfig::from_env();
    let batcher_config = BatcherConfig::from_env();

    let disk_buffer_enabled = env::var("ENABLE_DISK_BUFFER")
        .map(|s| s == "true" || s == "1")
        .unwrap_or(false);

    info!("Clickhouse URL: {}", ch_config.url);
    info!("Clickhouse table: {}.{}", ch_config.database, ch_config.table);
    info!("Buffer directory: {:?}", buffer_config.dir);
    info!("Disk buffer enabled: {}", disk_buffer_enabled);

    let insert_pool = InsertPool::new(ch_config)
        .await
        .expect("Failed to create Clickhouse insert pool");
    let insert_pool = Arc::new(insert_pool);

    let disk_buffer = MmapRingBuffer::new(buffer_config)
        .expect("Failed to create disk buffer");
    let disk_buffer = Arc::new(disk_buffer);

    let worker_count = batcher_config.worker_count;

    let (batcher, handle) = Batcher::new(
        batcher_config,
        Arc::clone(&insert_pool),
        Arc::clone(&disk_buffer),
    );

    let state = Arc::new(AppState {
        authenticator,
        batcher_handle: handle,
        disk_buffer,
        disk_buffer_enabled,
    });

    // Build router
    let app = Router::new()
        .route("/v1/traces", any(trace_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    let mut handles = Vec::new();
    let batcher = Arc::new(batcher);

    for i in 0..worker_count {
        let b = Arc::clone(&batcher);
        handles.push(tokio::spawn(async move {
            b.run_worker(i).await;
        }));
    }

    // Spawn diskbuffer drain
    {
        let drain_worker = batcher.create_drain_worker();
        handles.push(tokio::spawn(async move {
            drain_worker.run().await;
        }));
    }

    // Spawn process metrics update task
    {
        let b = Arc::clone(&batcher);
        handles.push(tokio::spawn(async move {
            let shutdown = b.shutdown_notify();
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        metrics::proc::update_process_metrics();
                    }
                    _ = shutdown.notified() => {
                        break;
                    }
                }
            }
        }));
    }

    // Start health server on separate port
    let health_port = env::var("HEALTH_PORT").unwrap_or_else(|_| "13133".to_string());
    let health_addr = format!("0.0.0.0:{}", health_port);
    let health_app = Router::new()
        .route("/", get(|| async { "OK" }))
        .route("/ready", get(ready_handler))
        .route("/health", get(|| async { "OK" }))
        .route("/debug/pprof/profile", get(metrics::pprof::profile_handler))
        .route("/debug/pprof/flamegraph", get(metrics::pprof::flamegraph_handler))
        .route("/debug/pprof/heap", get(metrics::heap::heap_handler))
        .route("/debug/pprof/stats", get(metrics::heap::stats_handler))
        .with_state(state.clone());

    let health_listener = tokio::net::TcpListener::bind(&health_addr)
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to bind health server to {}: {}. Is the port already in use?", health_addr, e)
        });
    info!("Health server listening on {}", health_addr);

    tokio::spawn(async move {
        if let Err(e) = axum::serve(health_listener, health_app).await {
            error!("Health server error: {}", e);
        }
    });

    // Start main server
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to bind main server to {}: {}. Is the port already in use?", addr, e)
        });

    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!("Main server error: {}", e);
    }

    info!("Signaling batcher shutdown...");
    batcher.shutdown();

    // Give workers time to drain (with timeout)
    let drain_timeout = Duration::from_secs(30);
    let drain_start = std::time::Instant::now();

    for handle in handles {
        let remaining = drain_timeout.saturating_sub(drain_start.elapsed());
        if remaining.is_zero() {
            warn!("Drain timeout reached, aborting remaining tasks");
            handle.abort();
        } else {
            match tokio::time::timeout(remaining, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!("Task panicked during shutdown: {}", e),
                Err(_) => warn!("Task did not complete within timeout"),
            }
        }
    }

    info!("Shutdown complete");
}

async fn trace_handler(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    let headers = req.headers().clone();

    // Detect content type
    let is_json = match headers.get(header::CONTENT_TYPE) {
        Some(ct) => match ct.to_str() {
            Ok(s) => s.contains("application/json"),
            Err(_) => {
                warn!("Content-Type header is not valid UTF-8, treating as protobuf");
                false
            }
        },
        None => {
            info!("No Content-Type header, defaulting to protobuf");
            false
        }
    };

    // Extract and validate auth
    let target_id = match auth::extract_and_validate(&state.authenticator, &headers).await {
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

    // Parse OTLP request
    let request = match proto::otlp::parse(&body_bytes, is_json) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to parse OTLP: {}", e);
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
    };

    // Transform to Clickhouse rows
    let rows = proto::transform::transform_request(&request, &target_id);
    let span_count = rows.len();
    let body_size = body_bytes.len();

    // Large requests go directly to disk to avoid OOM (if disk buffer enabled)
    if state.disk_buffer_enabled
        && (span_count > DIRECT_TO_DISK_THRESHOLD_SPANS || body_size > DIRECT_TO_DISK_THRESHOLD_BYTES)
    {
        info!(
            spans = span_count,
            body_size,
            target_id = %target_id,
            "Large request, buffering to disk"
        );
        let batch = BufferedBatch {
            rows,
            created_at_ns: current_time_nanos(),
        };
        return match state.disk_buffer.write_batch(batch) {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => {
                error!("Failed to buffer large request to disk: {}", e);
                (StatusCode::SERVICE_UNAVAILABLE, "Buffer full").into_response()
            }
        };
    }

    // Send to batcher (small requests)
    match state.batcher_handle.send(rows) {
        Ok(()) => {
            debug!(
                spans = span_count,
                target_id = %target_id,
                "Processed traces"
            );
            StatusCode::OK.into_response()
        }
        Err(BatcherError::ChannelFullWithData(rows)) => {
            if state.disk_buffer_enabled {
                // Try to buffer to disk
                let batch = BufferedBatch {
                    rows,
                    created_at_ns: current_time_nanos(),
                };
                match state.disk_buffer.write_batch(batch) {
                    Ok(()) => StatusCode::OK.into_response(),
                    Err(e) => {
                        error!("Failed to buffer to disk: {}", e);
                        (StatusCode::SERVICE_UNAVAILABLE, "Buffer full").into_response()
                    }
                }
            } else {
                (StatusCode::SERVICE_UNAVAILABLE, "Channel full").into_response()
            }
        }
        Err(BatcherError::Disconnected) => {
            error!("Batcher channel disconnected");
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal error").into_response()
        }
    }
}

async fn ready_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let level = state.batcher_handle.channel_len();
    if level < 95_000 {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready - backpressure")
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received, starting graceful shutdown");
}

fn current_time_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|e| {
            warn!("System clock appears to be before UNIX epoch: {}", e);
            std::time::Duration::ZERO
        })
        .as_nanos() as u64
}