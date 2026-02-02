use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tracing::info;

use crate::buffers::disk::BufferConfig as DiskBufferConfig;
use crate::exporters::clickhouse::{ClickHouseConfig as CHRuntimeConfig, BatcherConfig};
use crate::exporters::loki::{LokiConfig as LokiRuntimeConfig, LogBatcherConfig};
use crate::utils::limits::{default_buffer_size_with_source, BufferSizeSource};

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to parse config: {0}")]
    Parse(#[from] serde_yaml::Error),

    #[error("Invalid config: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub receivers: ReceiversConfig,

    #[serde(default)]
    pub exporters: ExportersConfig,

    #[serde(default)]
    pub pipelines: PipelinesConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_health_port")]
    pub health_port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            health_port: default_health_port(),
        }
    }
}

fn default_port() -> u16 {
    4318
}

fn default_health_port() -> u16 {
    13133
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ReceiversConfig {
    #[serde(default)]
    pub otlp: Option<OtlpReceiverConfig>,

    #[serde(default)]
    pub vercel: Option<VercelReceiverConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OtlpReceiverConfig {
    /// Endpoint for validating X-Hive-Target-Ref header
    #[serde(default)]
    pub auth_endpoint: Option<String>,

    /// Disable authentication (for testing)
    #[serde(default)]
    pub disable_auth: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VercelReceiverConfig {
    /// Shared secret for HMAC-SHA1 signature verification
    pub webhook_secret: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ExportersConfig {
    #[serde(default)]
    pub clickhouse: Option<ClickHouseExporterConfig>,

    #[serde(default)]
    pub loki: Option<LokiExporterConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseExporterConfig {
    /// ClickHouse URL (e.g., "http://clickhouse:8123")
    pub url: String,

    #[serde(default = "default_ch_database")]
    pub database: String,

    #[serde(default = "default_ch_table")]
    pub table: String,

    #[serde(default)]
    pub username: Option<String>,

    #[serde(default)]
    pub password: Option<String>,

    #[serde(default = "default_true")]
    pub async_insert: bool,

    #[serde(default)]
    pub wait_for_async_insert: bool,

    #[serde(default = "default_async_insert_max_data_size")]
    pub async_insert_max_data_size: u64,

    #[serde(default = "default_async_insert_busy_timeout_ms")]
    pub async_insert_busy_timeout_ms: u64,

    #[serde(default = "default_async_insert_max_query_number")]
    pub async_insert_max_query_number: u64,
}

fn default_ch_database() -> String {
    "default".to_string()
}

fn default_ch_table() -> String {
    "otel_traces".to_string()
}

fn default_true() -> bool {
    true
}

fn default_async_insert_max_data_size() -> u64 {
    10 * 1024 * 1024 // 10MB
}

fn default_async_insert_busy_timeout_ms() -> u64 {
    1000
}

fn default_async_insert_max_query_number() -> u64 {
    450
}

#[derive(Debug, Clone, Deserialize)]
pub struct LokiExporterConfig {
    /// Loki URL (e.g., "http://loki:3100")
    pub url: String,

    /// Tenant ID for multi-tenant Loki (X-Scope-OrgID header)
    #[serde(default)]
    pub tenant_id: Option<String>,

    #[serde(default)]
    pub username: Option<String>,

    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PipelinesConfig {
    #[serde(default)]
    pub traces: Option<TracePipelineConfig>,

    #[serde(default)]
    pub logs: Option<LogPipelineConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TracePipelineConfig {
    /// Receiver: "otlp"
    pub receiver: String,

    /// Exporter: "clickhouse"
    pub exporter: String,

    #[serde(default)]
    pub batch: BatchConfig,

    #[serde(default)]
    pub buffers: Option<BuffersConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct BuffersConfig {
    #[serde(default)]
    pub disk: Option<BufferConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogPipelineConfig {
    /// Receiver: "vercel"
    pub receiver: String,

    /// Exporter: "loki"
    pub exporter: String,

    #[serde(default)]
    pub batch: BatchConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchConfig {
    #[serde(default = "default_batch_size")]
    pub max_size: usize,

    #[serde(default = "default_batch_timeout_ms")]
    pub timeout_ms: u64,

    #[serde(default = "default_workers")]
    pub workers: usize,

    /// Memory buffer limit in MB (default: auto-detected from cgroup/system)
    #[serde(default)]
    pub mem_buffer_limit_mb: Option<u64>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: default_batch_size(),
            timeout_ms: default_batch_timeout_ms(),
            workers: default_workers(),
            mem_buffer_limit_mb: None,
        }
    }
}

fn default_batch_size() -> usize {
    10_000
}

fn default_batch_timeout_ms() -> u64 {
    200
}

fn default_workers() -> usize {
    4
}

#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,

    #[serde(default = "default_buffer_dir")]
    pub dir: String,

    #[serde(default = "default_buffer_max_size_mb")]
    pub max_size_mb: u64,

    #[serde(default = "default_buffer_segment_size_mb")]
    pub segment_size_mb: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dir: default_buffer_dir(),
            max_size_mb: default_buffer_max_size_mb(),
            segment_size_mb: default_buffer_segment_size_mb(),
        }
    }
}

fn default_buffer_dir() -> String {
    "/var/lib/otel-collector/buffer".to_string()
}

fn default_buffer_max_size_mb() -> u64 {
    1024
}

fn default_buffer_segment_size_mb() -> usize {
    64
}

// ============================================================================
// Conversion implementations: YAML config → Runtime config
// ============================================================================

impl ClickHouseExporterConfig {
    /// Convert to runtime ClickHouseConfig
    pub fn to_runtime_config(&self) -> CHRuntimeConfig {
        CHRuntimeConfig {
            url: self.url.clone(),
            database: self.database.clone(),
            table: self.table.clone(),
            username: self.username.clone().unwrap_or_else(|| "default".to_string()),
            password: self.password.clone().unwrap_or_default(),
            async_insert: self.async_insert,
            wait_for_async_insert: self.wait_for_async_insert,
            async_insert_max_data_size: self.async_insert_max_data_size,
            async_insert_busy_timeout_ms: self.async_insert_busy_timeout_ms,
            async_insert_max_query_number: self.async_insert_max_query_number,
            skip_unknown_fields: true,
            date_time_best_effort: true,
        }
    }
}

impl LokiExporterConfig {
    /// Convert to runtime LokiConfig
    pub fn to_runtime_config(&self) -> LokiRuntimeConfig {
        LokiRuntimeConfig {
            url: self.url.clone(),
            tenant_id: self.tenant_id.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            // Use defaults for settings not exposed in YAML
            timeout_secs: 30,
            max_retries: 3,
            retry_base_delay_ms: 100,
        }
    }
}

impl BatchConfig {
    /// Convert to runtime BatcherConfig for trace pipeline
    pub fn to_batcher_config(&self) -> BatcherConfig {
        let (mem_buffer_size_bytes, mem_buffer_size_source) = match self.mem_buffer_limit_mb {
            Some(mb) => ((mb as usize) * 1024 * 1024, BufferSizeSource::Config),
            None => default_buffer_size_with_source(),
        };
        BatcherConfig {
            max_batch_size: self.max_size,
            max_batch_timeout: Duration::from_millis(self.timeout_ms),
            worker_count: self.workers,
            mem_buffer_size_bytes,
            mem_buffer_size_source,
        }
    }

    /// Convert to runtime LogBatcherConfig for log pipeline
    pub fn to_log_batcher_config(&self) -> LogBatcherConfig {
        LogBatcherConfig {
            max_batch_size: self.max_size,
            max_batch_timeout: Duration::from_millis(self.timeout_ms),
            worker_count: self.workers,
        }
    }
}

impl BufferConfig {
    /// Convert to runtime DiskBufferConfig
    pub fn to_runtime_config(&self) -> DiskBufferConfig {
        DiskBufferConfig {
            dir: PathBuf::from(&self.dir),
            max_size: self.max_size_mb * 1024 * 1024,
            segment_size: self.segment_size_mb * 1024 * 1024,
        }
    }
}

impl Config {
    /// Load config from YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Load config from YAML string (used in tests)
    #[cfg(test)]
    pub fn from_str(yaml: &str) -> Result<Self, ConfigError> {
        let config: Config = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate configuration
    fn validate(&self) -> Result<(), ConfigError> {
        // Validate trace pipeline
        if let Some(ref traces) = self.pipelines.traces {
            if traces.receiver != "otlp" {
                return Err(ConfigError::Validation(format!(
                    "Unknown trace receiver: {}. Must be 'otlp'",
                    traces.receiver
                )));
            }
            if traces.exporter != "clickhouse" {
                return Err(ConfigError::Validation(format!(
                    "Unknown trace exporter: {}. Must be 'clickhouse'",
                    traces.exporter
                )));
            }
            if self.exporters.clickhouse.is_none() {
                return Err(ConfigError::Validation(
                    "Trace pipeline requires clickhouse exporter config".to_string(),
                ));
            }
        }

        // Validate log pipeline
        if let Some(ref logs) = self.pipelines.logs {
            if logs.receiver != "vercel" {
                return Err(ConfigError::Validation(format!(
                    "Unknown log receiver: {}. Must be 'vercel'",
                    logs.receiver
                )));
            }
            if logs.exporter != "loki" {
                return Err(ConfigError::Validation(format!(
                    "Unknown log exporter: {}. Must be 'loki'",
                    logs.exporter
                )));
            }
            if self.receivers.vercel.is_none() {
                return Err(ConfigError::Validation(
                    "Log pipeline requires vercel receiver config".to_string(),
                ));
            }
            if self.exporters.loki.is_none() {
                return Err(ConfigError::Validation(
                    "Log pipeline requires loki exporter config".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Print the configuration (masking secrets)
    pub fn print_config(&self) {
        info!("Server port: {}", self.server.port);
        info!("Health port: {}", self.server.health_port);

        if let Some(ref traces) = self.pipelines.traces {
            info!("Trace pipeline: {} -> {}", traces.receiver, traces.exporter);
            if let Some(ref ch) = self.exporters.clickhouse {
                info!("  ClickHouse: {}/{}.{}", ch.url, ch.database, ch.table);
            }
            info!(
                "  Batch: size={}, timeout={}ms, workers={}",
                traces.batch.max_size, traces.batch.timeout_ms, traces.batch.workers
            );
            if let Some(ref buffers) = traces.buffers {
                if let Some(ref disk) = buffers.disk {
                    info!(
                        "  Disk buffer: enabled={}, dir={}, max={}MB",
                        disk.enabled, disk.dir, disk.max_size_mb
                    );
                }
            }
        }

        if let Some(ref logs) = self.pipelines.logs {
            info!("Log pipeline: {} -> {}", logs.receiver, logs.exporter);
            if let Some(ref loki) = self.exporters.loki {
                info!("  Loki: {}", loki.url);
            }
            info!(
                "  Batch: size={}, timeout={}ms, workers={}",
                logs.batch.max_size, logs.batch.timeout_ms, logs.batch.workers
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_config() {
        let yaml = r#"
server:
  port: 4318
  health_port: 13133

receivers:
  otlp:
    auth_endpoint: "http://auth:4000/otel-auth"
  vercel:
    webhook_secret: "secret123"

exporters:
  clickhouse:
    url: "http://clickhouse:8123"
    database: "default"
    table: "otel_traces"
  loki:
    url: "http://loki:3100"
    tenant_id: "tenant1"

pipelines:
  traces:
    receiver: otlp
    exporter: clickhouse
    batch:
      max_size: 10000
      timeout_ms: 200
      workers: 4
    buffers:
      disk:
        enabled: true
        dir: "/var/lib/buffer"
        max_size_mb: 1024
  logs:
    receiver: vercel
    exporter: loki
    batch:
      max_size: 5000
      timeout_ms: 200
      workers: 2
"#;

        let config = Config::from_str(yaml).unwrap();

        assert_eq!(config.server.port, 4318);
        assert!(config.pipelines.traces.is_some());
        assert!(config.pipelines.logs.is_some());
        assert!(config.exporters.clickhouse.is_some());
        assert!(config.exporters.loki.is_some());
    }

    #[test]
    fn test_parse_traces_only() {
        let yaml = r#"
receivers:
  otlp:
    auth_endpoint: "http://auth:4000"

exporters:
  clickhouse:
    url: "http://clickhouse:8123"

pipelines:
  traces:
    receiver: otlp
    exporter: clickhouse
"#;

        let config = Config::from_str(yaml).unwrap();
        assert!(config.pipelines.traces.is_some());
        assert!(config.pipelines.logs.is_none());
    }

    #[test]
    fn test_parse_logs_only() {
        let yaml = r#"
receivers:
  vercel:
    webhook_secret: "secret"

exporters:
  loki:
    url: "http://loki:3100"

pipelines:
  logs:
    receiver: vercel
    exporter: loki
"#;

        let config = Config::from_str(yaml).unwrap();
        assert!(config.pipelines.traces.is_none());
        assert!(config.pipelines.logs.is_some());
    }

    #[test]
    fn test_validation_missing_exporter() {
        let yaml = r#"
pipelines:
  traces:
    receiver: otlp
    exporter: clickhouse
"#;

        let result = Config::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_invalid_receiver() {
        let yaml = r#"
exporters:
  clickhouse:
    url: "http://clickhouse:8123"

pipelines:
  traces:
    receiver: invalid
    exporter: clickhouse
"#;

        let result = Config::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_clickhouse_config_conversion() {
        let yaml = r#"
receivers:
  otlp:
    auth_endpoint: "http://auth:4000"

exporters:
  clickhouse:
    url: "http://clickhouse:8123"
    database: "mydb"
    table: "traces"
    username: "user"
    password: "pass"
    async_insert: false

pipelines:
  traces:
    receiver: otlp
    exporter: clickhouse
"#;

        let config = Config::from_str(yaml).unwrap();
        let ch = config.exporters.clickhouse.unwrap();
        let runtime = ch.to_runtime_config();

        assert_eq!(runtime.url, "http://clickhouse:8123");
        assert_eq!(runtime.database, "mydb");
        assert_eq!(runtime.table, "traces");
        assert_eq!(runtime.username, "user");
        assert_eq!(runtime.password, "pass");
        assert!(!runtime.async_insert);
    }

    #[test]
    fn test_loki_config_conversion() {
        let yaml = r#"
receivers:
  vercel:
    webhook_secret: "secret"

exporters:
  loki:
    url: "http://loki:3100"
    tenant_id: "tenant1"
    username: "admin"
    password: "secret"

pipelines:
  logs:
    receiver: vercel
    exporter: loki
"#;

        let config = Config::from_str(yaml).unwrap();
        let loki = config.exporters.loki.unwrap();
        let runtime = loki.to_runtime_config();

        assert_eq!(runtime.url, "http://loki:3100");
        assert_eq!(runtime.tenant_id, Some("tenant1".to_string()));
        assert_eq!(runtime.username, Some("admin".to_string()));
        assert_eq!(runtime.password, Some("secret".to_string()));
    }

    #[test]
    fn test_batch_config_conversion() {
        let batch = BatchConfig {
            max_size: 5000,
            timeout_ms: 100,
            workers: 8,
            mem_buffer_limit_mb: None,
        };

        let batcher = batch.to_batcher_config();
        assert_eq!(batcher.max_batch_size, 5000);
        assert_eq!(batcher.max_batch_timeout, std::time::Duration::from_millis(100));
        assert_eq!(batcher.worker_count, 8);

        let log_batcher = batch.to_log_batcher_config();
        assert_eq!(log_batcher.max_batch_size, 5000);
        assert_eq!(log_batcher.max_batch_timeout, std::time::Duration::from_millis(100));
        assert_eq!(log_batcher.worker_count, 8);
    }

    #[test]
    fn test_batch_config_with_mem_limit() {
        let batch = BatchConfig {
            max_size: 5000,
            timeout_ms: 100,
            workers: 8,
            mem_buffer_limit_mb: Some(512),
        };

        let batcher = batch.to_batcher_config();
        assert_eq!(batcher.mem_buffer_size_bytes, 512 * 1024 * 1024);
        assert!(matches!(batcher.mem_buffer_size_source, BufferSizeSource::Config));
    }

    #[test]
    fn test_buffer_config_conversion() {
        let buffer = BufferConfig {
            enabled: true,
            dir: "/tmp/buffer".to_string(),
            max_size_mb: 512,
            segment_size_mb: 32,
        };

        let runtime = buffer.to_runtime_config();
        assert_eq!(runtime.dir, std::path::PathBuf::from("/tmp/buffer"));
        assert_eq!(runtime.max_size, 512 * 1024 * 1024);
        assert_eq!(runtime.segment_size, 32 * 1024 * 1024);
    }
}
