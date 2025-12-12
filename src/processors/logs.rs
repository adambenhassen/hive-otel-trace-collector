use bytes::Bytes;
use tracing::{debug, error};

use crate::exporters::loki::{transform::parse_ndjson, LogBatcherHandle};

/// Process logs: parse and route to log batcher
pub async fn process_logs(
    log_handle: LogBatcherHandle,
    body: Bytes,
    target_id: String,
) {
    let logs = parse_ndjson(&body, &target_id);
    let log_count = logs.len();

    if logs.is_empty() {
        debug!(target_id = %target_id, "No valid logs parsed from Vercel payload");
        return;
    }

    // Send to log batcher
    match log_handle.send(logs) {
        Ok(()) => {
            debug!(
                logs = log_count,
                target_id = %target_id,
                "Processed Vercel logs"
            );
        }
        Err(_logs) => {
            let log_count = _logs.len();
            error!(
                logs = log_count,
                target_id = %target_id,
                "Log batcher channel closed, logs lost"
            );
        }
    }
}
