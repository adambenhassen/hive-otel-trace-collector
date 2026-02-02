use bytes::Bytes;
use tracing::{debug, error};

use crate::exporters::loki::{transform::parse_ndjson, LogBatcherHandle};

/// Process logs: parse and route to Loki exporter
pub async fn process_logs(
    loki_handle: Option<LogBatcherHandle>,
    body: Bytes,
) {
    let logs = parse_ndjson(&body);
    let log_count = logs.len();

    if logs.is_empty() {
        debug!("No valid logs parsed from Vercel payload");
        return;
    }

    // Send to Loki if configured
    if let Some(handle) = loki_handle {
        match handle.send(logs) {
            Ok(()) => {
                debug!(logs = log_count, "Sent logs to Loki batcher");
            }
            Err(_logs) => {
                error!(logs = _logs.len(), "Loki batcher channel closed, logs lost");
            }
        }
    }
}
