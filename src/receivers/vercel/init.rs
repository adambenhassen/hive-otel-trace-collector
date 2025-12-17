use std::sync::Arc;

use axum::{routing::post, Router};
use tracing::info;

use crate::config::Config;
use crate::exporters::loki::LokiExporter;
use crate::exporters::kafka::KafkaExporter;
use super::{vercel_log_handler, VercelHandlerState, VercelSignatureVerifier};

pub struct VercelReceiver;

impl VercelReceiver {
    pub fn try_init(
        config: &Config,
        loki: Option<&LokiExporter>,
        kafka: Option<&KafkaExporter>,
    ) -> Option<Router> {
        // Require at least one exporter
        if loki.is_none() && kafka.is_none() {
            return None;
        }

        let vercel_config = config.receivers.vercel.as_ref()?;

        let vercel_verifier = VercelSignatureVerifier::new(
            vercel_config.webhook_secret.clone()
        );

        let state = Arc::new(VercelHandlerState {
            vercel_verifier,
            loki_handle: loki.map(|l| l.handle.clone()),
            kafka_handle: kafka.map(|k| k.handle.clone()),
        });

        info!(
            loki_enabled = loki.is_some(),
            kafka_enabled = kafka.is_some(),
            "Vercel receiver initialized"
        );

        Some(Router::new()
            .route("/v1/logs/vercel", post(vercel_log_handler))
            .with_state(state))
    }
}
