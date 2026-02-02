use std::sync::Arc;

use axum::{routing::post, Router};
use tracing::info;

use crate::config::Config;
use crate::exporters::loki::LokiExporter;
use super::{vercel_log_handler, VercelHandlerState, VercelSignatureVerifier};

pub struct VercelReceiver;

impl VercelReceiver {
    pub fn try_init(
        config: &Config,
        loki: Option<&LokiExporter>,
    ) -> Option<Router> {
        // Require Loki exporter
        let loki = loki?;

        let vercel_config = config.receivers.vercel.as_ref()?;

        let vercel_verifier = VercelSignatureVerifier::new(
            vercel_config.webhook_secret.clone()
        );

        let state = Arc::new(VercelHandlerState {
            vercel_verifier,
            loki_handle: Some(loki.handle.clone()),
        });

        info!("Vercel receiver initialized");

        Some(Router::new()
            .route("/v1/logs/vercel", post(vercel_log_handler))
            .with_state(state))
    }
}
