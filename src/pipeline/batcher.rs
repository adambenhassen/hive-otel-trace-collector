use crate::diskbuffer::{BufferedBatch, MmapRingBuffer};
use crate::metrics::mem_limit::{default_buffer_size_with_source, BufferSizeSource};
use crate::pipeline::clickhouse::InsertPool;
use crate::proto::span::Span;
use async_channel::{Receiver, Sender};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct BatcherConfig {
    pub max_batch_size: usize,
    pub max_batch_timeout: Duration,
    pub worker_count: usize,
    pub mem_buffer_size_bytes: usize,
    pub mem_buffer_size_source: BufferSizeSource,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        let (mem_buffer_size_bytes, mem_buffer_size_source) = default_buffer_size_with_source();
        Self {
            max_batch_size: 10_000,
            max_batch_timeout: Duration::from_millis(200),
            worker_count: 4,
            mem_buffer_size_bytes,
            mem_buffer_size_source,
        }
    }
}

impl BatcherConfig {
    pub fn from_env() -> Self {
        // Check if MEM_BUFFER_SIZE_MB is set in environment
        let (mem_buffer_size_bytes, mem_buffer_size_source) =
            match std::env::var("MEM_BUFFER_SIZE_MB") {
                Ok(s) => match s.parse::<usize>() {
                    Ok(mb) => (mb * 1024 * 1024, BufferSizeSource::Env),
                    Err(_) => {
                        warn!(value = %s, "Invalid MEM_BUFFER_SIZE_MB, using auto-detected value");
                        default_buffer_size_with_source()
                    }
                },
                Err(_) => default_buffer_size_with_source(),
            };

        let max_batch_size = match std::env::var("BATCH_MAX_SPANS") {
            Ok(s) => match s.parse::<usize>() {
                Ok(v) if v > 0 => v,
                Ok(_) => {
                    warn!(value = %s, "BATCH_MAX_SPANS must be positive, using default 10000");
                    10_000
                }
                Err(_) => {
                    warn!(value = %s, "Invalid BATCH_MAX_SPANS, using default 10000");
                    10_000
                }
            },
            Err(_) => 10_000,
        };

        let batch_timeout_ms = match std::env::var("BATCH_TIMEOUT_MS") {
            Ok(s) => match s.parse::<u64>() {
                Ok(v) if v > 0 => v,
                Ok(_) => {
                    warn!(value = %s, "BATCH_TIMEOUT_MS must be positive, using default 200");
                    200
                }
                Err(_) => {
                    warn!(value = %s, "Invalid BATCH_TIMEOUT_MS, using default 200");
                    200
                }
            },
            Err(_) => 200,
        };

        let worker_count = match std::env::var("BATCH_WORKERS") {
            Ok(s) => match s.parse::<usize>() {
                Ok(v) if v > 0 => v,
                Ok(_) => {
                    warn!(value = %s, "BATCH_WORKERS must be positive, using default 4");
                    4
                }
                Err(_) => {
                    warn!(value = %s, "Invalid BATCH_WORKERS, using default 4");
                    4
                }
            },
            Err(_) => 4,
        };

        Self {
            max_batch_size,
            max_batch_timeout: Duration::from_millis(batch_timeout_ms),
            worker_count,
            mem_buffer_size_bytes,
            mem_buffer_size_source,
        }
    }

    pub fn mem_buffer_limit_mb(&self) -> u64 {
        (self.mem_buffer_size_bytes / (1024 * 1024)) as u64
    }
}

#[derive(Clone)]
pub struct BatcherHandle {
    sender: Sender<Vec<Span>>,
}

impl BatcherHandle {
    pub fn send(&self, rows: Vec<Span>) -> Result<(), Vec<Span>> {
        self.sender
            .try_send(rows)
            .map_err(|e| e.into_inner())
    }
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
        let (sender, receiver) = async_channel::unbounded();

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
