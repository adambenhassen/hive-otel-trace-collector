use std::sync::Arc;
use tracing::info;

use crate::config::Config;
use super::{LokiClient, LogBatcher, LogBatcherHandle};

pub struct LokiExporter {
    pub batcher: Arc<LogBatcher>,
    pub handle: LogBatcherHandle,
}

impl LokiExporter {
    pub fn try_init(config: &Config) -> Option<(Self, Vec<tokio::task::JoinHandle<()>>)> {
        let loki_config = config.exporters.loki.as_ref()?;

        let runtime_config = loki_config.to_runtime_config();

        // Get batch config from logs pipeline if configured, otherwise use defaults
        let log_batcher_config = config.pipelines.logs.as_ref()
            .map(|l| l.batch.to_log_batcher_config())
            .unwrap_or_default();

        let loki_client = LokiClient::new(runtime_config)
            .expect("Failed to create Loki client");
        let loki_client = Arc::new(loki_client);

        let (batcher, handle) = LogBatcher::new(
            log_batcher_config,
            loki_client,
        );

        let batcher = Arc::new(batcher);
        let handles = batcher.spawn_workers();

        info!("Loki exporter initialized");

        Some((Self { batcher, handle }, handles))
    }
}
