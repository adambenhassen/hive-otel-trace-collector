use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use axum::Router;

use crate::config::Config;
use crate::exporters::Exporters;
use super::otlp::OtlpReceiver;
use super::vercel::VercelReceiver;

pub struct Receivers;

impl Receivers {
    pub fn init(
        config: &Config,
        exporters: &Exporters,
        in_backpressure: Arc<AtomicBool>,
    ) -> Router {
        let mut app = Router::new();

        // Get mem_buffer_limit from traces pipeline config
        let mem_buffer_limit_mb = config.pipelines.traces.as_ref()
            .map(|t| t.batch.to_batcher_config().mem_buffer_limit_mb())
            .unwrap_or(0);

        // Initialize OTLP receiver if configured (requires ClickHouse exporter)
        if let Some(ref ch) = exporters.clickhouse {
            if let Some(router) = OtlpReceiver::try_init(
                config,
                ch,
                mem_buffer_limit_mb,
                Arc::clone(&in_backpressure),
            ) {
                app = app.merge(router);
            }
        }

        // Initialize Vercel receiver if configured (requires Loki exporter)
        if let Some(router) = VercelReceiver::try_init(
            config,
            exporters.loki.as_ref(),
        ) {
            app = app.merge(router);
        }

        app
    }
}
