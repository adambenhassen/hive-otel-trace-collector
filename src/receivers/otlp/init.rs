use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use axum::{routing::any, Router};
use reqwest::Client;
use tracing::{info, warn};
use url::Url;

use crate::config::Config;
use crate::exporters::clickhouse::ClickHouseExporter;
use super::{trace_handler, Authenticator, TraceHandlerState, is_auth_disabled};

// Threshold for writing directly to disk (skip channel to avoid OOM)
const DIRECT_TO_DISK_THRESHOLD_BYTES: usize = 1024 * 1024; // 1MB

pub struct OtlpReceiver;

impl OtlpReceiver {
    pub fn try_init(
        config: &Config,
        clickhouse: &ClickHouseExporter,
        mem_buffer_limit_mb: u64,
        in_backpressure: Arc<AtomicBool>,
    ) -> Option<Router> {
        let otlp_config = config.receivers.otlp.as_ref()?;

        let auth_endpoint = otlp_config.auth_endpoint.clone()
            .unwrap_or_else(|| "http://graphql-api:4000/otel-auth".to_string());

        // Set DISABLE_AUTH env var if configured
        if otlp_config.disable_auth {
            // SAFETY: This is called during single-threaded initialization before spawning workers
            unsafe { std::env::set_var("DISABLE_AUTH", "true") };
        }

        // Only validate auth_endpoint URL if auth is enabled
        if !is_auth_disabled() {
            Url::parse(&auth_endpoint).expect("auth_endpoint is not a valid URL");
        }

        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        let authenticator = Authenticator::new(http_client, auth_endpoint);
        if is_auth_disabled() {
            warn!("Authentication is DISABLED");
        }

        let state = Arc::new(TraceHandlerState {
            authenticator,
            batcher_handle: clickhouse.handle.clone(),
            disk_buffer: clickhouse.batcher.disk_buffer(),
            mem_buffer_limit_mb,
            direct_to_disk_threshold_bytes: DIRECT_TO_DISK_THRESHOLD_BYTES,
            in_backpressure,
        });

        info!("OTLP receiver initialized");

        Some(Router::new()
            .route("/v1/traces", any(trace_handler))
            .with_state(state))
    }
}
