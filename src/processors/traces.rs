use std::sync::Arc;

use bytes::Bytes;
use tracing::{debug, error, warn};

use crate::buffers::disk::{BufferedBatch, MmapRingBuffer};
use crate::exporters::clickhouse::BatcherHandle;
use crate::utils;
use crate::receivers::otlp::{parse, transform};

/// Configuration for trace processing decisions
pub struct TraceProcessorConfig {
    pub disk_buffer: Option<Arc<MmapRingBuffer>>,
    pub batcher_handle: BatcherHandle,
    pub mem_buffer_limit_mb: u64,
    pub direct_to_disk_threshold_bytes: usize,
}

/// Process traces: parse, transform, and route to batcher or disk buffer
pub async fn process_traces(
    config: &TraceProcessorConfig,
    body_bytes: Bytes,
    is_json: bool,
    target_id: String,
) {
    // Parse OTLP request
    let request = match parse::parse(&body_bytes, is_json) {
        Ok(req) => req,
        Err(e) => {
            warn!(target_id = %target_id, "Failed to parse OTLP: {}", e);
            return;
        }
    };

    // Transform to Clickhouse rows
    let rows = transform::transform_request(&request, &target_id);
    let span_count = rows.len();
    let body_size = body_bytes.len();

    // Check if we should buffer to disk based on:
    // 1. Large request size (>1MB)
    // 2. Process memory exceeds limit
    let process_mem_mb = utils::proc::get_mem_mb();
    let should_buffer_to_disk = config.disk_buffer.is_some()
        && (body_size > config.direct_to_disk_threshold_bytes
            || process_mem_mb >= config.mem_buffer_limit_mb);

    if should_buffer_to_disk {
        if let Some(ref disk_buffer) = config.disk_buffer {
            debug!(
                spans = span_count,
                body_size,
                process_mem_mb,
                mem_limit_mb = config.mem_buffer_limit_mb,
                target_id = %target_id,
                "Buffering to disk"
            );
            let batch = BufferedBatch {
                rows,
                created_at_ns: current_time_nanos(),
            };
            if let Err(e) = disk_buffer.write_batch(batch) {
                error!(target_id = %target_id, error = %e, "Failed to buffer to disk");
            }
            return;
        }
    }

    // Send to batcher
    match config.batcher_handle.send(rows) {
        Ok(()) => {
            debug!(
                spans = span_count,
                target_id = %target_id,
                "Processed traces"
            );
        }
        Err(rows) => {
            // Channel closed (shutdown) try disk buffer
            let span_count = rows.len();
            debug!(
                spans = span_count,
                target_id = %target_id,
                "Batcher channel closed, attempting disk buffer fallback"
            );

            if let Some(ref disk_buffer) = config.disk_buffer {
                let batch = BufferedBatch {
                    rows,
                    created_at_ns: current_time_nanos(),
                };
                if let Err(e) = disk_buffer.write_batch(batch) {
                    error!(spans = span_count, target_id = %target_id, error = %e, "Failed to buffer to disk");
                }
            } else {
                error!(spans = span_count, target_id = %target_id, "Channel closed and disk buffer disabled, data lost");
            }
        }
    }
}

fn current_time_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|e| {
            warn!("System clock appears to be before UNIX epoch: {}", e);
            std::time::Duration::ZERO
        })
        .as_nanos() as u64
}
