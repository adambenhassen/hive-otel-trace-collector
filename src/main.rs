#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod buffers;
mod config;
mod exporters;
mod utils;
mod processors;
mod receivers;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use config::Config;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// State for health check endpoints
struct HealthState {
    mem_buffer_limit_mb: u64,
    in_backpressure: Arc<AtomicBool>,
}

#[tokio::main]
async fn main() {
    // Set default log level
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .with_target(false)
        .init();

    // Load configuration from file
    let config_path = std::env::args()
        .skip_while(|arg| arg != "--config")
        .nth(1)
        .or_else(|| env::var("CONFIG_FILE").ok())
        .unwrap_or_else(|| "config.yaml".to_string());
    let config = Config::from_file(&config_path)
        .unwrap_or_else(|e| panic!("Failed to load config from {}: {}", config_path, e));

    info!("Loaded config from {}", config_path);
    config.print_config();

    info!("Starting collector on {}", format!("0.0.0.0:{}", config.server.port));

    let in_backpressure = Arc::new(AtomicBool::new(false));

    // Initialize exporters
    let (exporters, handles) = exporters::Exporters::init(&config).await;

    // Initialize receivers
    let app = receivers::Receivers::init(
        &config,
        &exporters,
        Arc::clone(&in_backpressure),
    );

    // Log http calls (middleware)
    let app = app.layer(TraceLayer::new_for_http());

    // Spawn process metrics update task
    utils::proc::spawn_metrics_task();

    // Get mem_buffer_limit from clickhouse batcher config
    let mem_buffer_limit_mb = config.pipelines.traces.as_ref()
        .map(|t| t.batch.to_batcher_config().mem_buffer_limit_mb())
        .unwrap_or(0);

    // Start health server
    let health_state = Arc::new(HealthState {
        mem_buffer_limit_mb,
        in_backpressure: Arc::clone(&in_backpressure),
    });
    spawn_server(&format!("0.0.0.0:{}", config.server.health_port), health_routes(health_state), "Health").await;

    // Start main server
    let main_addr = format!("0.0.0.0:{}", config.server.port);
    let listener = tokio::net::TcpListener::bind(&main_addr)
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to bind main server to {}: {}. Is the port already in use?", main_addr, e)
        });
    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!("Main server error: {}", e);
    }

    // Shutdown
    info!("Signaling shutdown...");
    exporters.shutdown();

    // Give workers time to drain
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

fn health_routes(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/", get(|| async { "OK" }))
        .route("/ready", get(ready_handler))
        .route("/health", get(|| async { "OK" }))
        .route("/debug/pprof/profile", get(utils::debug::profile_handler))
        .route("/debug/pprof/flamegraph", get(utils::debug::flamegraph_handler))
        .route("/debug/pprof/heap", get(utils::debug::heap_handler))
        .route("/debug/pprof/stats", get(utils::debug::stats_handler))
        .with_state(state)
}

async fn ready_handler(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let process_mem_mb = utils::proc::get_mem_mb();
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

async fn spawn_server(addr: &str, app: Router, name: &'static str) {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to bind {} server to {}: {}. Is the port already in use?", name, addr, e)
        });
    info!("{} server listening on {}", name, addr);

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            error!("{} server error: {}", name, e);
        }
    });
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