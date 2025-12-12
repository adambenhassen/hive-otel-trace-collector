pub mod buffers;
pub mod config;
pub mod exporters;
pub mod utils;
pub mod processors;
pub mod receivers;

// Re-exports for public API
pub use buffers::disk::{BufferConfig, BufferedBatch, MmapRingBuffer};
pub use exporters::clickhouse::{ClickHouseConfig, InsertPool, Batcher, BatcherConfig, BatcherHandle};
pub use exporters::loki::{LokiClient, LokiConfig, LogBatcher, LogBatcherConfig, LogBatcherHandle};
pub use processors::{TraceProcessorConfig, process_traces, process_logs};
pub use receivers::otlp::{Span, Authenticator};
pub use receivers::vercel::{VercelSignatureVerifier, VercelLogEntry, LokiLogEntry, LokiLabels};
