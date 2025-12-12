use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use reqwest::Client;
use thiserror::Error;
use tracing::{error, info, warn};

use crate::receivers::vercel::LokiLogEntry;
use super::transform::{group_logs_for_push, LokiPushRequest};

#[derive(Error, Debug)]
pub enum LokiError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Loki returned error: {status} - {body}")]
    LokiResponse { status: u16, body: String },

    #[error("Request timeout")]
    #[allow(dead_code)]
    Timeout,
}

#[derive(Debug, Clone)]
pub struct LokiConfig {
    /// Base URL for Loki (e.g., "http://loki:3100")
    pub url: String,

    /// Optional tenant ID for multi-tenant Loki (X-Scope-OrgID header)
    pub tenant_id: Option<String>,

    /// Optional basic auth username
    pub username: Option<String>,

    /// Optional basic auth password
    pub password: Option<String>,

    /// Request timeout in seconds
    pub timeout_secs: u64,

    /// Maximum retries on failure
    pub max_retries: u32,

    /// Base delay for exponential backoff (ms)
    pub retry_base_delay_ms: u64,
}

impl Default for LokiConfig {
    fn default() -> Self {
        Self {
            url: "http://loki:3100".to_string(),
            tenant_id: None,
            username: None,
            password: None,
            timeout_secs: 30,
            max_retries: 3,
            retry_base_delay_ms: 100,
        }
    }
}

impl LokiConfig {
    /// Load configuration from environment variables (deprecated, use config file instead)
    #[allow(dead_code)]
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("LOKI_URL").unwrap_or_else(|_| "http://loki:3100".to_string()),
            tenant_id: std::env::var("LOKI_TENANT_ID").ok(),
            username: std::env::var("LOKI_USERNAME").ok(),
            password: std::env::var("LOKI_PASSWORD").ok(),
            timeout_secs: std::env::var("LOKI_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            max_retries: std::env::var("LOKI_MAX_RETRIES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
            retry_base_delay_ms: std::env::var("LOKI_RETRY_BASE_DELAY_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
        }
    }

    /// Get the push endpoint URL
    pub fn push_url(&self) -> String {
        format!("{}/loki/api/v1/push", self.url.trim_end_matches('/'))
    }
}

pub struct LokiClient {
    client: Client,
    config: LokiConfig,
    healthy: AtomicBool,
}

impl LokiClient {
    pub fn new(config: LokiConfig) -> Result<Self, LokiError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()?;

        info!(url = %config.url, "Loki client initialized");

        Ok(Self {
            client,
            config,
            healthy: AtomicBool::new(true),
        })
    }

    /// Push logs to Loki with retry logic.
    pub async fn push(&self, logs: &[LokiLogEntry]) -> Result<(), LokiError> {
        if logs.is_empty() {
            return Ok(());
        }

        let request = group_logs_for_push(logs);
        self.push_request(&request).await
    }

    /// Push a pre-formatted request to Loki with retry logic.
    async fn push_request(&self, request: &LokiPushRequest) -> Result<(), LokiError> {
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                let delay_ms =
                    self.config.retry_base_delay_ms * (2_u64.pow(attempt.saturating_sub(1)));
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }

            match self.send_push_request(request).await {
                Ok(()) => {
                    self.mark_healthy();
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        attempt = attempt + 1,
                        max_retries = self.config.max_retries,
                        error = %e,
                        "Loki push failed"
                    );
                    last_error = Some(e);
                }
            }
        }

        self.mark_unhealthy();
        Err(last_error.unwrap())
    }

    async fn send_push_request(&self, request: &LokiPushRequest) -> Result<(), LokiError> {
        let mut req = self
            .client
            .post(self.config.push_url())
            .header("Content-Type", "application/json")
            .json(request);

        // Add tenant header if configured
        if let Some(ref tenant_id) = self.config.tenant_id {
            req = req.header("X-Scope-OrgID", tenant_id);
        }

        // Add basic auth if configured
        if let (Some(username), Some(password)) =
            (&self.config.username, &self.config.password)
        {
            req = req.basic_auth(username, Some(password));
        }

        let response = req.send().await?;
        let status = response.status();

        if status.is_success() {
            Ok(())
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(LokiError::LokiResponse {
                status: status.as_u16(),
                body,
            })
        }
    }

    /// Check if the client is healthy (last push succeeded).
    #[allow(dead_code)]
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn mark_healthy(&self) {
        if !self.healthy.swap(true, Ordering::SeqCst) {
            info!("Loki connection recovered");
        }
    }

    fn mark_unhealthy(&self) {
        if self.healthy.swap(false, Ordering::SeqCst) {
            error!("Loki connection unhealthy");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env() {
        // Clear any existing env vars for clean test
        // SAFETY: Test-only, single-threaded test environment
        unsafe {
            std::env::remove_var("LOKI_URL");
            std::env::remove_var("LOKI_TENANT_ID");
        }

        let config = LokiConfig::from_env();
        assert_eq!(config.url, "http://loki:3100");
        assert!(config.tenant_id.is_none());
        assert_eq!(config.timeout_secs, 30);
    }

    #[test]
    fn test_push_url() {
        let config = LokiConfig {
            url: "http://loki:3100".to_string(),
            ..Default::default()
        };
        assert_eq!(config.push_url(), "http://loki:3100/loki/api/v1/push");

        let config_trailing = LokiConfig {
            url: "http://loki:3100/".to_string(),
            ..Default::default()
        };
        assert_eq!(
            config_trailing.push_url(),
            "http://loki:3100/loki/api/v1/push"
        );
    }

    #[test]
    fn test_client_creation() {
        let config = LokiConfig::default();
        let client = LokiClient::new(config);
        assert!(client.is_ok());
    }
}
