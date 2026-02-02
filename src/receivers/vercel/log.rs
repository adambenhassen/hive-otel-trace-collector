use serde::{Deserialize, Serialize};

/// Vercel Log Drain NDJSON format.
///
/// See: https://vercel.com/docs/drains/reference/logs
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VercelLogEntry {
    /// Unique log entry ID
    pub id: String,

    /// Log message content
    #[serde(default)]
    pub message: Option<String>,

    /// Unix timestamp in milliseconds
    pub timestamp: i64,

    /// Log type: "stdout", "stderr", "request", "edge-request", etc.
    #[serde(rename = "type", default)]
    pub log_type: Option<String>,

    /// Source: "build", "static", "lambda", "edge", "external"
    pub source: String,

    /// Vercel project ID
    pub project_id: String,

    /// Vercel project name
    #[serde(default)]
    pub project_name: Option<String>,

    /// Deployment ID
    pub deployment_id: String,

    /// Git branch
    #[serde(default)]
    pub branch: Option<String>,

    /// Environment: "production", "preview", "development"
    #[serde(default)]
    pub environment: Option<String>,

    /// Request ID (present for request logs)
    #[serde(default)]
    pub request_id: Option<String>,

    /// HTTP status code (present for request logs)
    #[serde(default)]
    pub status_code: Option<u16>,

    /// Request host
    #[serde(default)]
    pub host: Option<String>,

    /// Request path
    #[serde(default)]
    pub path: Option<String>,

    /// Log level: "info", "warn", "error"
    #[serde(default)]
    pub level: Option<String>,

    /// Execution region
    #[serde(default)]
    pub execution_region: Option<String>,

    /// HTTP method
    #[serde(default)]
    pub method: Option<String>,

    /// User agent
    #[serde(default)]
    pub user_agent: Option<String>,

    /// Proxy/edge information
    #[serde(default)]
    pub proxy: Option<ProxyInfo>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyInfo {
    #[serde(default)]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub status_code: Option<u16>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub client_ip: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub lambda_region: Option<String>,
    #[serde(default)]
    pub path_type: Option<String>,
    #[serde(default)]
    pub scheme: Option<String>,
    #[serde(default)]
    pub vercel_cache: Option<String>,
}

/// Internal log entry representation for batching and disk buffer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LokiLogEntry {
    /// Timestamp in nanoseconds
    pub timestamp_ns: i64,

    /// Log line content (JSON-serialized)
    pub line: String,

    /// Loki labels for indexing
    pub labels: LokiLabels,
}

/// Loki labels - low cardinality fields for indexing.
///
/// High-cardinality fields should go in the log line, not labels.
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct LokiLabels {
    /// Vercel project ID
    pub project_id: String,

    /// Source: "build", "static", "lambda", "edge"
    pub source: String,

    /// Log type: "stdout", "stderr", "request", etc.
    pub log_type: String,

    /// Log level: "info", "warn", "error"
    pub level: String,
}

impl LokiLabels {
    /// Convert labels to a HashMap for Loki push format.
    pub fn to_hashmap(&self) -> std::collections::HashMap<String, String> {
        let mut map = std::collections::HashMap::new();
        map.insert("project_id".to_string(), self.project_id.clone());
        map.insert("source".to_string(), self.source.clone());
        map.insert("log_type".to_string(), self.log_type.clone());
        map.insert("level".to_string(), self.level.clone());
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_vercel_log_entry() {
        let json = r#"{
            "id": "log_123",
            "message": "Hello world",
            "timestamp": 1702400000000,
            "type": "stdout",
            "source": "lambda",
            "projectId": "prj_abc",
            "deploymentId": "dpl_xyz",
            "requestId": "req_456",
            "statusCode": 200,
            "host": "example.com",
            "path": "/api/test"
        }"#;

        let entry: VercelLogEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.id, "log_123");
        assert_eq!(entry.message, Some("Hello world".to_string()));
        assert_eq!(entry.timestamp, 1702400000000);
        assert_eq!(entry.log_type, Some("stdout".to_string()));
        assert_eq!(entry.source, "lambda");
        assert_eq!(entry.project_id, "prj_abc");
        assert_eq!(entry.deployment_id, "dpl_xyz");
        assert_eq!(entry.request_id, Some("req_456".to_string()));
        assert_eq!(entry.status_code, Some(200));
        assert_eq!(entry.host, Some("example.com".to_string()));
        assert_eq!(entry.path, Some("/api/test".to_string()));
    }

    #[test]
    fn test_parse_minimal_vercel_log() {
        let json = r#"{
            "id": "log_123",
            "message": "Test",
            "timestamp": 1702400000000,
            "type": "stderr",
            "source": "build",
            "projectId": "prj_abc",
            "deploymentId": "dpl_xyz"
        }"#;

        let entry: VercelLogEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.id, "log_123");
        assert_eq!(entry.log_type, Some("stderr".to_string()));
        assert_eq!(entry.source, "build");
        assert!(entry.request_id.is_none());
        assert!(entry.status_code.is_none());
    }

    #[test]
    fn test_loki_labels_to_hashmap() {
        let labels = LokiLabels {
            project_id: "prj_abc".to_string(),
            source: "lambda".to_string(),
            log_type: "stdout".to_string(),
            level: "info".to_string(),
        };

        let map = labels.to_hashmap();
        assert_eq!(map.get("project_id"), Some(&"prj_abc".to_string()));
        assert_eq!(map.get("source"), Some(&"lambda".to_string()));
        assert_eq!(map.get("log_type"), Some(&"stdout".to_string()));
        assert_eq!(map.get("level"), Some(&"info".to_string()));
    }
}
