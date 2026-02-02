use std::collections::HashMap;

use serde::Serialize;

use crate::receivers::vercel::{LokiLabels, LokiLogEntry, VercelLogEntry};

/// Loki Push API request format.
/// POST /loki/api/v1/push
#[derive(Debug, Serialize)]
pub struct LokiPushRequest {
    pub streams: Vec<LokiStream>,
}

/// A stream of log entries sharing the same label set.
#[derive(Debug, Serialize)]
pub struct LokiStream {
    pub stream: HashMap<String, String>,
    pub values: Vec<[String; 2]>,
}

/// Transform a Vercel log entry to internal Loki format.
///
/// - Extracts low-cardinality fields as labels
/// - Serializes full log content as JSON line
/// - Converts timestamp from ms to ns
pub fn transform_vercel_to_loki(vercel_log: &VercelLogEntry) -> LokiLogEntry {
    // Default log_type based on source if not present
    let log_type = vercel_log.log_type.clone()
        .unwrap_or_else(|| vercel_log.source.clone());

    // Derive level if not present
    let level = vercel_log.level.clone().unwrap_or_else(|| {
        match log_type.as_str() {
            "stderr" => "error".to_string(),
            _ => "info".to_string(),
        }
    });

    let labels = LokiLabels {
        project_id: vercel_log.project_id.clone(),
        source: vercel_log.source.clone(),
        log_type,
        level,
    };

    // Convert timestamp from ms to ns
    let timestamp_ns = vercel_log.timestamp * 1_000_000;

    // Create structured log line as JSON with full details
    let line = serde_json::json!({
        "id": vercel_log.id,
        "message": vercel_log.message,
        "projectName": vercel_log.project_name,
        "deploymentId": vercel_log.deployment_id,
        "branch": vercel_log.branch,
        "environment": vercel_log.environment,
        "requestId": vercel_log.request_id,
        "statusCode": vercel_log.status_code,
        "host": vercel_log.host,
        "path": vercel_log.path,
        "method": vercel_log.method,
        "userAgent": vercel_log.user_agent,
        "region": vercel_log.execution_region,
        "proxy": vercel_log.proxy,
    })
    .to_string();

    LokiLogEntry {
        timestamp_ns,
        line,
        labels,
    }
}

/// Group logs by label set for Loki push format.
///
/// Loki requires logs to be grouped by their label set in each push request.
pub fn group_logs_for_push(logs: &[LokiLogEntry]) -> LokiPushRequest {
    let mut streams: HashMap<LokiLabels, Vec<[String; 2]>> = HashMap::new();

    for log in logs {
        let values = streams.entry(log.labels.clone()).or_default();
        values.push([log.timestamp_ns.to_string(), log.line.clone()]);
    }

    LokiPushRequest {
        streams: streams
            .into_iter()
            .map(|(labels, values)| LokiStream {
                stream: labels.to_hashmap(),
                values,
            })
            .collect(),
    }
}

/// Parse NDJSON body into Vercel log entries.
///
/// Skips invalid lines instead of failing the entire batch.
pub fn parse_ndjson(body: &[u8]) -> Vec<LokiLogEntry> {
    body.split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .filter_map(|line| {
            serde_json::from_slice::<VercelLogEntry>(line)
                .ok()
                .map(|v| transform_vercel_to_loki(&v))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vercel_log() -> VercelLogEntry {
        VercelLogEntry {
            id: "log_123".to_string(),
            message: Some("Hello world".to_string()),
            timestamp: 1702400000000,
            log_type: Some("stdout".to_string()),
            source: "lambda".to_string(),
            project_id: "prj_abc".to_string(),
            project_name: Some("my-app".to_string()),
            deployment_id: "dpl_xyz".to_string(),
            branch: Some("main".to_string()),
            environment: Some("production".to_string()),
            request_id: Some("req_456".to_string()),
            status_code: Some(200),
            host: Some("example.com".to_string()),
            path: Some("/api/test".to_string()),
            level: None,
            execution_region: Some("iad1".to_string()),
            method: Some("GET".to_string()),
            user_agent: None,
            proxy: None,
        }
    }

    #[test]
    fn test_transform_vercel_to_loki() {
        let vercel_log = make_vercel_log();
        let loki_entry = transform_vercel_to_loki(&vercel_log);

        assert_eq!(loki_entry.timestamp_ns, 1702400000000 * 1_000_000);
        assert_eq!(loki_entry.labels.project_id, "prj_abc");
        assert_eq!(loki_entry.labels.source, "lambda");
        assert_eq!(loki_entry.labels.log_type, "stdout");
        assert_eq!(loki_entry.labels.level, "info");

        // Verify log line is valid JSON
        let line_json: serde_json::Value = serde_json::from_str(&loki_entry.line).unwrap();
        assert_eq!(line_json["id"], "log_123");
        assert_eq!(line_json["message"], "Hello world");
    }

    #[test]
    fn test_level_derived_from_stderr() {
        let mut vercel_log = make_vercel_log();
        vercel_log.log_type = Some("stderr".to_string());
        vercel_log.level = None;

        let loki_entry = transform_vercel_to_loki(&vercel_log);
        assert_eq!(loki_entry.labels.level, "error");
    }

    #[test]
    fn test_explicit_level_preserved() {
        let mut vercel_log = make_vercel_log();
        vercel_log.level = Some("warn".to_string());

        let loki_entry = transform_vercel_to_loki(&vercel_log);
        assert_eq!(loki_entry.labels.level, "warn");
    }

    #[test]
    fn test_group_logs_for_push() {
        let logs = vec![
            LokiLogEntry {
                timestamp_ns: 1000,
                line: "log1".to_string(),
                labels: LokiLabels {
                    project_id: "prj_a".to_string(),
                    source: "lambda".to_string(),
                    log_type: "stdout".to_string(),
                    level: "info".to_string(),
                },
            },
            LokiLogEntry {
                timestamp_ns: 2000,
                line: "log2".to_string(),
                labels: LokiLabels {
                    project_id: "prj_a".to_string(),
                    source: "lambda".to_string(),
                    log_type: "stdout".to_string(),
                    level: "info".to_string(),
                },
            },
            LokiLogEntry {
                timestamp_ns: 3000,
                line: "log3".to_string(),
                labels: LokiLabels {
                    project_id: "prj_b".to_string(),
                    source: "edge".to_string(),
                    log_type: "stderr".to_string(),
                    level: "error".to_string(),
                },
            },
        ];

        let request = group_logs_for_push(&logs);

        // Should have 2 streams (different label sets)
        assert_eq!(request.streams.len(), 2);

        // Find the stream for prj_a
        let prj_a_stream = request
            .streams
            .iter()
            .find(|s| s.stream.get("project_id") == Some(&"prj_a".to_string()))
            .unwrap();
        assert_eq!(prj_a_stream.values.len(), 2);

        // Find the stream for prj_b
        let prj_b_stream = request
            .streams
            .iter()
            .find(|s| s.stream.get("project_id") == Some(&"prj_b".to_string()))
            .unwrap();
        assert_eq!(prj_b_stream.values.len(), 1);
    }

    #[test]
    fn test_parse_ndjson() {
        let ndjson = r#"{"id":"1","message":"a","timestamp":1000,"type":"stdout","source":"lambda","projectId":"prj","deploymentId":"dpl"}
{"id":"2","message":"b","timestamp":2000,"type":"stderr","source":"edge","projectId":"prj","deploymentId":"dpl"}
invalid json line
{"id":"3","message":"c","timestamp":3000,"type":"stdout","source":"build","projectId":"prj","deploymentId":"dpl"}"#;

        let logs = parse_ndjson(ndjson.as_bytes());

        // Should parse 3 valid lines, skip the invalid one
        assert_eq!(logs.len(), 3);
        assert_eq!(logs[0].labels.log_type, "stdout");
        assert_eq!(logs[1].labels.log_type, "stderr");
        assert_eq!(logs[2].labels.log_type, "stdout");
    }

    #[test]
    fn test_parse_ndjson_empty() {
        let logs = parse_ndjson(b"");
        assert!(logs.is_empty());
    }
}
