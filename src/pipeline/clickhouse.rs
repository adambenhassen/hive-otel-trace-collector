use crate::metrics::proc as metrics;
use crate::proto::span::Span;
use clickhouse::{Client, Compression};
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;
use tracing::{info, warn};

#[derive(Error, Debug)]
pub enum InsertError {
    #[error("Clickhouse error: {0}")]
    Clickhouse(#[from] clickhouse::error::Error),
}

#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub table: String,
    pub username: String,
    pub password: String,
    pub async_insert: bool,
    pub wait_for_async_insert: bool,
    pub async_insert_max_data_size: u64,
    pub async_insert_busy_timeout_ms: u64,
    pub async_insert_max_query_number: u64,
    pub skip_unknown_fields: bool,
    pub date_time_best_effort: bool,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: "http://clickhouse:8123".to_string(),
            database: "default".to_string(),
            table: "otel_traces".to_string(),
            username: "default".to_string(),
            password: String::new(),
            async_insert: true,
            wait_for_async_insert: false,
            async_insert_max_data_size: 10 * 1024 * 1024, // 10MB
            async_insert_busy_timeout_ms: 1000,
            async_insert_max_query_number: 450,
            skip_unknown_fields: true,
            date_time_best_effort: true,
        }
    }
}

impl ClickHouseConfig {
    pub fn from_env() -> Self {
        let url = if let (Ok(host), Ok(port)) = (
            std::env::var("CLICKHOUSE_HOST"),
            std::env::var("CLICKHOUSE_PORT"),
        ) {
            let protocol = std::env::var("CLICKHOUSE_PROTOCOL").unwrap_or_else(|_| "http".to_string());
            format!("{}://{}:{}", protocol, host, port)
        } else {
            std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://clickhouse:8123".to_string())
        };

        Self {
            url,
            database: std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string()),
            table: std::env::var("CLICKHOUSE_TABLE").unwrap_or_else(|_| "otel_traces".to_string()),
            username: std::env::var("CLICKHOUSE_USERNAME").unwrap_or_else(|_| "default".to_string()),
            password: std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            async_insert: std::env::var("CLICKHOUSE_ASYNC_INSERT")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
            wait_for_async_insert: std::env::var("CLICKHOUSE_WAIT_FOR_ASYNC_INSERT")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(false),
            async_insert_max_data_size: std::env::var("CLICKHOUSE_ASYNC_INSERT_MAX_DATA_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10 * 1024 * 1024),
            async_insert_busy_timeout_ms: std::env::var("CLICKHOUSE_ASYNC_INSERT_BUSY_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
            async_insert_max_query_number: std::env::var("CLICKHOUSE_ASYNC_INSERT_MAX_QUERY_NUMBER")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(450),
            skip_unknown_fields: std::env::var("CLICKHOUSE_SKIP_UNKNOWN_FIELDS")
                .map(|s| s != "false" && s != "0")
                .unwrap_or(true),
            date_time_best_effort: std::env::var("CLICKHOUSE_DATE_TIME_BEST_EFFORT")
                .map(|s| s != "false" && s != "0")
                .unwrap_or(true),
        }
    }
}

pub struct InsertPool {
    client: Client,
    table: String,
    healthy: AtomicBool,
}

impl InsertPool {
    pub async fn new(config: ClickHouseConfig) -> Result<Self, InsertError> {
        let mut client = Client::default()
            .with_url(&config.url)
            .with_database(&config.database)
            .with_user(&config.username)
            .with_password(&config.password)
            .with_compression(Compression::Lz4);

        if config.skip_unknown_fields {
            client = client.with_option("input_format_skip_unknown_fields", "1");
        }
        if config.date_time_best_effort {
            client = client.with_option("date_time_input_format", "best_effort");
        }

        client = client.with_option("optimize_on_insert", "0");

        if config.async_insert {
            client = client
                .with_option("async_insert", "1")
                .with_option(
                    "async_insert_max_data_size",
                    &config.async_insert_max_data_size.to_string(),
                )
                .with_option(
                    "async_insert_busy_timeout_ms",
                    &config.async_insert_busy_timeout_ms.to_string(),
                )
                .with_option(
                    "async_insert_max_query_number",
                    &config.async_insert_max_query_number.to_string(),
                );
            if config.wait_for_async_insert {
                client = client.with_option("wait_for_async_insert", "1");
            } else {
                client = client.with_option("wait_for_async_insert", "0");
            }
        }

        info!(
            url = %config.url,
            table = %config.table,
            "Created clickhouse client"
        );

        Ok(Self {
            client,
            table: config.table,
            healthy: AtomicBool::new(true),
        })
    }

    pub async fn insert(&self, rows: &[Span]) -> Result<(), InsertError> {
        if rows.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();
        let spans = rows.len();
        let mut insert = self.client.insert::<Span>(&self.table).await?;

        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;

        let ch_ms = start.elapsed().as_millis();
        let mem_mb = metrics::get_mem_mb();
        let cpu_m = metrics::get_cpu_m();
        info!(cpu_m, mem_mb, spans, ch_ms, "traces inserted");
        self.healthy.store(true, Ordering::Relaxed);

        Ok(())
    }
}
