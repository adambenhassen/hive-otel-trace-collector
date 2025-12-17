use std::sync::Arc;
use tracing::info;

use crate::config::Config;
use super::{KafkaClient, KafkaLogBatcher, KafkaLogBatcherHandle};

pub struct KafkaExporter {
    pub batcher: Arc<KafkaLogBatcher>,
    pub handle: KafkaLogBatcherHandle,
}

impl KafkaExporter {
    pub fn try_init(config: &Config) -> Option<(Self, Vec<tokio::task::JoinHandle<()>>)> {
        let kafka_config = config.exporters.kafka.as_ref()?;

        let runtime_config = kafka_config.to_runtime_config();

        // Get batch config from logs pipeline if configured, otherwise use defaults
        let kafka_batcher_config = config
            .pipelines
            .logs
            .as_ref()
            .map(|l| l.batch.to_kafka_log_batcher_config())
            .unwrap_or_default();

        let kafka_client = KafkaClient::new(runtime_config)
            .expect("Failed to create Kafka client");
        let kafka_client = Arc::new(kafka_client);

        let (batcher, handle) = KafkaLogBatcher::new(kafka_batcher_config, kafka_client);

        let batcher = Arc::new(batcher);
        let handles = batcher.spawn_workers();

        info!("Kafka exporter initialized");

        Some((Self { batcher, handle }, handles))
    }
}
