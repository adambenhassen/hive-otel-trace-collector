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
use bytes::Bytes;
use diskbuffer::{BufferConfig, BufferedBatch, MmapRingBuffer};
use pipeline::{Batcher, BatcherConfig, BatcherHandle, ClickHouseConfig, InsertPool};
use reqwest::Client;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{env, sync::Arc, time::Duration};
use tokio::signal;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;

const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30); // Request body read timeout

// Threshold for writing directly to disk (skip channel to avoid OOM)
const DIRECT_TO_DISK_THRESHOLD_BYTES: usize = 1024 * 1024; // 1MB

struct AppState {
    authenticator: Authenticator,
    batcher_handle: BatcherHandle,
    disk_buffer: Arc<MmapRingBuffer>,
    disk_buffer_enabled: bool,
    mem_buffer_limit_mb: u64,
    in_backpressure: AtomicBool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
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
    if auth::is_auth_disabled() {
        warn!("Authentication is DISABLED (DISABLE_AUTH=true)");
    }

    let ch_config = ClickHouseConfig::from_env();
    let buffer_config = BufferConfig::from_env();
    let batcher_config = BatcherConfig::from_env();

    let disk_buffer_enabled = env::var("DISK_BUFFER_ENABLED")
        .map(|s| s != "false" && s != "0")
        .unwrap_or(true);

    info!("Clickhouse URL: {}", ch_config.url);
    info!("Clickhouse table: {}.{}", ch_config.database, ch_config.table);
    info!("Buffer directory: {:?}", buffer_config.dir);
    info!("Disk buffer enabled: {}", disk_buffer_enabled);
    info!(
        "Memory buffer limit: {} MB ({})",
        batcher_config.mem_buffer_size_bytes / (1024 * 1024),
        batcher_config.mem_buffer_size_source
    );

    let insert_pool = InsertPool::new(ch_config)
        .await
        .expect("Failed to create Clickhouse insert pool");
    let insert_pool = Arc::new(insert_pool);

    let disk_buffer = MmapRingBuffer::new(buffer_config)
        .expect("Failed to create disk buffer");
    let disk_buffer = Arc::new(disk_buffer);

    let worker_count = batcher_config.worker_count;
    let mem_buffer_limit_mb = batcher_config.mem_buffer_limit_mb();

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
        mem_buffer_limit_mb,
        in_backpressure: AtomicBool::new(false),
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

    // Spawn diskbuffer drain workers (same count as batch workers)
    for drain_id in 0..worker_count {
        let drain_worker = batcher.create_drain_worker();
        handles.push(tokio::spawn(async move {
            drain_worker.run(drain_id).await;
        }));
    }

    // Spawn process metrics update task
    {
        let b = Arc::clone(&batcher);
        handles.push(tokio::spawn(async move {
            let shutdown = b.shutdown_notify();
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
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

    let state_clone = state.clone();
    tokio::spawn(async move {
        process_trace_batch(state_clone, body_bytes, is_json, target_id).await;
    });
    StatusCode::OK.into_response()
}

async fn process_trace_batch(
    state: Arc<AppState>,
    body_bytes: Bytes,
    is_json: bool,
    target_id: String,
) {
    // Parse OTLP request
    let request = match proto::otlp::parse(&body_bytes, is_json) {
        Ok(req) => req,
        Err(e) => {
            warn!(target_id = %target_id, "Failed to parse OTLP: {}", e);
            return;
        }
    };

    // Transform to Clickhouse rows
    let rows = proto::transform::transform_request(&request, &target_id);
    let span_count = rows.len();
    let body_size = body_bytes.len();

    // Check if we should buffer to disk based on:
    // 1. Large request size (>1MB)
    // 2. Process memory exceeds limit
    let process_mem_mb = metrics::proc::get_mem_mb();
    let should_buffer_to_disk = state.disk_buffer_enabled
        && (body_size > DIRECT_TO_DISK_THRESHOLD_BYTES
            || process_mem_mb >= state.mem_buffer_limit_mb);

    if should_buffer_to_disk {
        debug!(
            spans = span_count,
            body_size,
            process_mem_mb,
            mem_limit_mb = state.mem_buffer_limit_mb,
            target_id = %target_id,
            "Buffering to disk"
        );
        let batch = BufferedBatch {
            rows,
            created_at_ns: current_time_nanos(),
        };
        if let Err(e) = state.disk_buffer.write_batch(batch) {
            error!(target_id = %target_id, error = %e, "Failed to buffer to disk");
        }
        return;
    }

    // Send to batcher
    match state.batcher_handle.send(rows) {
        Ok(()) => {
            debug!(
                spans = span_count,
                target_id = %target_id,
                "Processed traces"
            );
        }
        Err(rows) => {
            // Channel closed (shutdown) try disk buffer
            let span_count = rows.len();
            debug!(
                spans = span_count,
                target_id = %target_id,
                "Batcher channel closed, attempting disk buffer fallback"
            );

            if state.disk_buffer_enabled {
                let batch = BufferedBatch {
                    rows,
                    created_at_ns: current_time_nanos(),
                };
                if let Err(e) = state.disk_buffer.write_batch(batch) {
                    error!(spans = span_count, target_id = %target_id, error = %e, "Failed to buffer to disk");
                }
            } else {
                error!(spans = span_count, target_id = %target_id, "Channel closed and disk buffer disabled, data lost");
            }
        }
    }
}

async fn ready_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let process_mem_mb = metrics::proc::get_mem_mb();
    let high_threshold = (state.mem_buffer_limit_mb * 95) / 100; // 95% - become unready
    let low_threshold = (state.mem_buffer_limit_mb * 85) / 100;  // 85% - become ready again

    let was_in_backpressure = state.in_backpressure.load(Ordering::Relaxed);

    // Hysteresis: once in backpressure, stay until memory drops below low threshold
    let is_in_backpressure = if was_in_backpressure {
        // Stay in backpressure until memory drops below 85%
        process_mem_mb >= low_threshold
    } else {
        // Enter backpressure when memory exceeds 95%
        process_mem_mb >= high_threshold
    };

    // Update state if changed
    if is_in_backpressure != was_in_backpressure {
        state.in_backpressure.store(is_in_backpressure, Ordering::Relaxed);
    }

    if is_in_backpressure {
        return (StatusCode::SERVICE_UNAVAILABLE, "not ready, memory backpressure");
    }

    (StatusCode::OK, "ready")
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