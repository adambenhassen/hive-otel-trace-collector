use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_channel::{Receiver, Sender};
use tokio::sync::Notify;
use tracing::{error, info};

use super::LokiClient;
use crate::receivers::vercel::LokiLogEntry;

#[derive(Debug, Clone)]
pub struct LogBatcherConfig {
    pub max_batch_size: usize,
    pub max_batch_timeout: Duration,
    pub worker_count: usize,
}

impl Default for LogBatcherConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 5_000,
            max_batch_timeout: Duration::from_millis(200),
            worker_count: 2,
        }
    }
}

impl LogBatcherConfig {
    /// Load configuration from environment variables (deprecated, use config file instead)
    #[allow(dead_code)]
    pub fn from_env() -> Self {
        let max_batch_size = std::env::var("LOG_BATCH_MAX_ENTRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5_000);

        let batch_timeout_ms = std::env::var("LOG_BATCH_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200u64);

        let worker_count = std::env::var("LOG_BATCH_WORKERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);

        Self {
            max_batch_size,
            max_batch_timeout: Duration::from_millis(batch_timeout_ms),
            worker_count,
        }
    }
}

#[derive(Clone)]
pub struct LogBatcherHandle {
    sender: Sender<Vec<LokiLogEntry>>,
}

impl LogBatcherHandle {
    pub fn send(&self, logs: Vec<LokiLogEntry>) -> Result<(), Vec<LokiLogEntry>> {
        self.sender.try_send(logs).map_err(|e| e.into_inner())
    }
}

pub struct LogBatcher {
    config: LogBatcherConfig,
    receiver: Receiver<Vec<LokiLogEntry>>,
    loki_client: Arc<LokiClient>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
}

impl LogBatcher {
    pub fn new(
        config: LogBatcherConfig,
        loki_client: Arc<LokiClient>,
    ) -> (Self, LogBatcherHandle) {
        let (sender, receiver) = async_channel::unbounded();

        let handle = LogBatcherHandle { sender };

        let batcher = Self {
            config,
            receiver,
            loki_client,
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
        };

        (batcher, handle)
    }

    /// Spawn all log batcher workers, returning the join handles
    pub fn spawn_workers(self: &Arc<Self>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();
        let worker_count = self.config.worker_count;

        for i in 0..worker_count {
            let batcher = Arc::clone(self);
            handles.push(tokio::spawn(async move {
                batcher.run_worker(i).await;
            }));
        }

        handles
    }

    async fn run_worker(&self, worker_id: usize) {
        info!(worker_id, "Log batch worker started");

        let mut batch: Vec<LokiLogEntry> = Vec::with_capacity(self.config.max_batch_size);
        let mut last_flush = Instant::now();

        loop {
            if self.is_shutdown() && self.receiver.is_empty() {
                break;
            }

            let elapsed = last_flush.elapsed();
            let recv_timeout = self
                .config
                .max_batch_timeout
                .saturating_sub(elapsed)
                .max(Duration::from_millis(10));

            match tokio::time::timeout(recv_timeout, self.receiver.recv()).await {
                Ok(Ok(logs)) => {
                    batch.extend(logs);

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

        info!(worker_id, "Log batch worker stopped");
    }

    async fn flush_batch(&self, batch: &mut Vec<LokiLogEntry>, worker_id: usize) {
        if batch.is_empty() {
            return;
        }

        let count = batch.len();
        let logs = std::mem::take(batch);

        let start = Instant::now();
        match self.loki_client.push(&logs).await {
            Ok(()) => {
                let elapsed_ms = start.elapsed().as_millis();
                info!(worker_id, logs = count, elapsed_ms, "Pushed logs to Loki");
            }
            Err(e) => {
                let elapsed_ms = start.elapsed().as_millis();
                error!(
                    worker_id,
                    error = %e,
                    logs = count,
                    elapsed_ms,
                    "Loki push failed, logs lost"
                );
            }
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}
