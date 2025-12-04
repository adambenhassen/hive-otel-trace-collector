use crate::diskbuffer::MmapRingBuffer;
use crate::pipeline::clickhouse::InsertPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

pub struct DrainWorker {
    insert_pool: Arc<InsertPool>,
    disk_buffer: Arc<MmapRingBuffer>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
}

impl DrainWorker {
    pub fn new(
        insert_pool: Arc<InsertPool>,
        disk_buffer: Arc<MmapRingBuffer>,
        shutdown: Arc<AtomicBool>,
        shutdown_notify: Arc<Notify>,
    ) -> Self {
        Self {
            insert_pool,
            disk_buffer,
            shutdown,
            shutdown_notify,
        }
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    async fn interruptible_sleep(&self, duration: Duration) {
        tokio::select! {
            _ = tokio::time::sleep(duration) => {}
            _ = self.shutdown_notify.notified() => {}
        }
    }

    pub async fn run(&self, drain_id: usize) {
        info!(drain_id, "Disk buffer drain started");

        loop {
            if self.is_shutdown() && !self.disk_buffer.has_pending() {
                break;
            }

            // Only drain if we have pending data
            let pending_entries = self.disk_buffer.pending_entries();
            let pending_bytes = self.disk_buffer.pending_bytes();
            if pending_entries == 0 {
                if self.is_shutdown() {
                    break; // No data and shutting down
                }
                self.interruptible_sleep(Duration::from_millis(500)).await;
                continue;
            }

            debug!(
                pending_entries,
                pending_bytes, "Drain: found pending data"
            );

            match self.disk_buffer.read_batch() {
                Ok(Some(batch)) => {
                    let count = batch.rows.len();
                    if let Err(e) = self.insert_pool.insert(&batch.rows).await {
                        warn!(error = %e, spans = count, "Drain: insert failed, re-buffering");
                        // Re-buffer the batch to avoid data loss
                        if let Err(buf_err) = self.disk_buffer.write_batch(batch) {
                            error!(error = %buf_err, spans = count, "Failed to re-buffer batch, data lost!");
                        }
                    }
                }
                Ok(None) => {
                    warn!(
                        pending_entries,
                        "Drain: pending_entries > 0 but read_batch returned None, recomputing counters"
                    );
                    self.disk_buffer.recompute_counters();
                }
                Err(e) => {
                    error!(error = %e, "Drain: error reading from disk buffer");
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("Disk buffer drain stopped");
    }
}
