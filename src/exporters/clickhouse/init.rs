use std::sync::Arc;
use tracing::info;

use crate::config::Config;
use super::{Batcher, BatcherHandle};

pub struct ClickHouseExporter {
    pub batcher: Arc<Batcher>,
    pub handle: BatcherHandle,
}

impl ClickHouseExporter {
    pub async fn try_init(config: &Config) -> Option<(Self, Vec<tokio::task::JoinHandle<()>>)> {
        let ch_config = config.exporters.clickhouse.as_ref()?;

        let runtime_config = ch_config.to_runtime_config();

        // Get batch config from traces pipeline if configured, otherwise use defaults
        let batcher_config = config.pipelines.traces.as_ref()
            .map(|t| t.batch.to_batcher_config())
            .unwrap_or_default();

        // Get disk buffer config from traces pipeline if configured
        let buffer_config = config.pipelines.traces.as_ref()
            .and_then(|t| t.buffers.as_ref())
            .and_then(|b| b.disk.as_ref())
            .filter(|d| d.enabled)
            .map(|d| d.to_runtime_config());

        let (batcher, handle) = Batcher::new(
            runtime_config,
            batcher_config,
            buffer_config,
        ).await.expect("Failed to create ClickHouse batcher");

        let batcher = Arc::new(batcher);
        let handles = batcher.spawn_workers();

        info!("ClickHouse exporter initialized");

        Some((Self { batcher, handle }, handles))
    }
}
