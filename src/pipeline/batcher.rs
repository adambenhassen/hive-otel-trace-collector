use crate::diskbuffer::{BufferedBatch, MmapRingBuffer};
use crate::pipeline::clickhouse::InsertPool;
use crate::proto::span::Span;
use async_channel::{Receiver, Sender, TrySendError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct BatcherConfig {
    pub max_batch_size: usize,
    pub max_batch_timeout: Duration,
    pub channel_capacity: usize,
    pub worker_count: usize,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 10_000,
            max_batch_timeout: Duration::from_millis(200),
            channel_capacity: 100_000,
            worker_count: 4,
        }
    }
}

impl BatcherConfig {
    pub fn from_env() -> Self {
        Self {
            max_batch_size: std::env::var("BATCH_MAX_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10_000),
            max_batch_timeout: Duration::from_millis(
                std::env::var("BATCH_TIMEOUT_MS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(200),
            ),
            channel_capacity: std::env::var("CHANNEL_CAPACITY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100_000),
            worker_count: std::env::var("BATCHER_WORKERS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(4),
        }
    }
}

#[derive(Clone)]
pub struct BatcherHandle {
    sender: Sender<Vec<Span>>,
}

impl BatcherHandle {
    pub fn send(&self, rows: Vec<Span>) -> Result<(), BatcherError> {
        match self.sender.try_send(rows) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(rows)) => Err(BatcherError::ChannelFullWithData(rows)),
            Err(TrySendError::Closed(_)) => Err(BatcherError::Disconnected),
        }
    }

    /// Get current channel length
    pub fn channel_len(&self) -> usize {
        self.sender.len()
    }
}

#[derive(Debug)]
pub enum BatcherError {
    ChannelFullWithData(Vec<Span>),
    Disconnected,
}

pub struct Batcher {
    config: BatcherConfig,
    receiver: Receiver<Vec<Span>>,
    insert_pool: Arc<InsertPool>,
    disk_buffer: Arc<MmapRingBuffer>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
}

impl Batcher {
    pub fn new(
        config: BatcherConfig,
        insert_pool: Arc<InsertPool>,
        disk_buffer: Arc<MmapRingBuffer>,
    ) -> (Self, BatcherHandle) {
        let (sender, receiver) = async_channel::bounded(config.channel_capacity);

        let handle = BatcherHandle { sender };

        let batcher = Self {
            config,
            receiver,
            insert_pool,
            disk_buffer,
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
        };

        (batcher, handle)
    }

    pub fn create_drain_worker(&self) -> super::DrainWorker {
        super::DrainWorker::new(
            Arc::clone(&self.insert_pool),
            Arc::clone(&self.disk_buffer),
            Arc::clone(&self.shutdown),
            Arc::clone(&self.shutdown_notify),
        )
    }

    pub async fn run_worker(&self, worker_id: usize) {
        info!(worker_id, "Batch worker started");

        let mut batch: Vec<Span> = Vec::with_capacity(self.config.max_batch_size);
        let mut last_flush = Instant::now();

        loop {
            if self.is_shutdown() && self.receiver.is_empty() {
                break;
            }

            // Calculate remaining time until batch should flush
            let elapsed = last_flush.elapsed();
            let recv_timeout = self.config
                .max_batch_timeout
                .saturating_sub(elapsed)
                .max(Duration::from_millis(10));

            match tokio::time::timeout(recv_timeout, self.receiver.recv()).await {
                Ok(Ok(rows)) => {
                    batch.extend(rows);

                    // Flush if batch is full
                    if batch.len() >= self.config.max_batch_size {
                        self.flush_batch(&mut batch, worker_id).await;
                        last_flush = Instant::now();
                    }
                }
                Ok(Err(_)) => {
                    // Channel closed, flush remaining and exit
                    if !batch.is_empty() {
                        self.flush_batch(&mut batch, worker_id).await;
                    }
                    break;
                }
                Err(_) => {
                    // Timeout, flush if we have data
                    if !batch.is_empty() && last_flush.elapsed() >= self.config.max_batch_timeout {
                        self.flush_batch(&mut batch, worker_id).await;
                        last_flush = Instant::now();
                    }
                }
            }
        }

        if !batch.is_empty() {
            self.flush_batch(&mut batch, worker_id).await;
        }

        info!(worker_id, "Batch worker stopped");
    }

    async fn flush_batch(&self, batch: &mut Vec<Span>, worker_id: usize) {
        if batch.is_empty() {
            return;
        }

        let count = batch.len();
        let rows = std::mem::take(batch);

        // Try to insert to Clickhouse
        if let Err(e) = self.insert_pool.insert(&rows).await {
            warn!(
                worker_id,
                error = %e,
                spans = count,
                "Clickhouse insert failed, buffering to disk"
            );

            // Buffer to disk
            let buffered_batch = BufferedBatch {
                rows,
                created_at_ns: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64,
            };

            if let Err(e) = self.disk_buffer.write_batch(buffered_batch) {
                error!(
                    worker_id,
                    error = %e,
                    spans = count,
                    "Failed to buffer to disk, data lost!"
                );
            }
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
    }

    pub fn shutdown_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.shutdown_notify)
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batcher_config_default() {
        let config = BatcherConfig::default();
        assert_eq!(config.max_batch_size, 10_000);
        assert_eq!(config.channel_capacity, 100_000);
    }
}
