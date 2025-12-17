use bytes::Bytes;
use tracing::{debug, error};

use crate::exporters::loki::{transform::parse_ndjson, LogBatcherHandle};
use crate::exporters::kafka::{transform::from_loki_entries, KafkaLogBatcherHandle};

/// Process logs: parse and route to configured exporters (Loki, Kafka, or both)
pub async fn process_logs(
    loki_handle: Option<LogBatcherHandle>,
    kafka_handle: Option<KafkaLogBatcherHandle>,
    body: Bytes,
    target_id: String,
) {
    let logs = parse_ndjson(&body, &target_id);
    let log_count = logs.len();

    if logs.is_empty() {
        debug!(target_id = %target_id, "No valid logs parsed from Vercel payload");
        return;
    }

    // Send to Loki if configured
    if let Some(handle) = loki_handle {
        match handle.send(logs.clone()) {
            Ok(()) => {
                debug!(
                    logs = log_count,
                    target_id = %target_id,
                    "Sent logs to Loki batcher"
                );
            }
            Err(_logs) => {
                error!(
                    logs = _logs.len(),
                    target_id = %target_id,
                    "Loki batcher channel closed, logs lost"
                );
            }
        }
    }

    // Send to Kafka if configured
    if let Some(handle) = kafka_handle {
        let kafka_logs = from_loki_entries(&logs);
        match handle.send(kafka_logs) {
            Ok(()) => {
                debug!(
                    logs = log_count,
                    target_id = %target_id,
                    "Sent logs to Kafka batcher"
                );
            }
            Err(_logs) => {
                error!(
                    logs = _logs.len(),
                    target_id = %target_id,
                    "Kafka batcher channel closed, logs lost"
                );
            }
        }
    }
}
