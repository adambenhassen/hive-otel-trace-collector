mod common;

use common::CREATE_TABLE_SQL;
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue, KeyValue},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use prost::Message;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::clickhouse::ClickHouse;

struct TestContext {
    #[allow(dead_code)]
    clickhouse: ContainerAsync<ClickHouse>,
    #[allow(dead_code)]
    buffer_dir: TempDir,
    #[allow(dead_code)]
    log_dir: TempDir,
    collector_pid: u32,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    http_client: reqwest::Client,
    ch_client: clickhouse::Client,
    traces_url: String,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Kill collector process on drop to prevent orphaned processes on test panic
        unsafe {
            libc::kill(self.collector_pid as i32, libc::SIGKILL);
        }

        // If panicking, print collector logs for debugging
        if std::thread::panicking() {
            eprintln!("\n=== Collector stdout ===");
            if let Ok(stdout) = std::fs::read_to_string(&self.stdout_path) {
                eprintln!("{}", stdout);
            }
            eprintln!("\n=== Collector stderr ===");
            if let Ok(stderr) = std::fs::read_to_string(&self.stderr_path) {
                eprintln!("{}", stderr);
            }
            eprintln!("========================\n");
        }
    }
}

impl TestContext {
    async fn new() -> Self {
        // Start ClickHouse
        let clickhouse = ClickHouse::default()
            .with_tag("24.8")
            .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
            .start()
            .await
            .expect("Failed to start ClickHouse");

        let ch_host = clickhouse.get_host().await.unwrap();
        let ch_port = clickhouse.get_host_port_ipv4(8123).await.unwrap();
        let ch_url = format!("http://{}:{}", ch_host, ch_port);

        let ch_client = clickhouse::Client::default()
            .with_url(&ch_url)
            .with_database("default");

        ch_client
            .query(CREATE_TABLE_SQL)
            .execute()
            .await
            .expect("Failed to create table");

        // Find available ports
        let collector_port = portpicker::pick_unused_port().expect("No free port");
        let health_port = portpicker::pick_unused_port().expect("No free port");

        let buffer_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let log_dir = tempfile::tempdir().expect("Failed to create log dir");
        let stdout_path = log_dir.path().join("stdout.log");
        let stderr_path = log_dir.path().join("stderr.log");

        let stdout_file = std::fs::File::create(&stdout_path).expect("Failed to create stdout log");
        let stderr_file = std::fs::File::create(&stderr_path).expect("Failed to create stderr log");

        // Start collector
        let collector =
            tokio::process::Command::new(env!("CARGO_BIN_EXE_hive-otel-trace-collector"))
                .env("PORT", collector_port.to_string())
                .env("HEALTH_PORT", health_port.to_string())
                .env("CLICKHOUSE_URL", &ch_url)
                .env("CLICKHOUSE_DATABASE", "default")
                .env("CLICKHOUSE_TABLE", "otel_traces")
                .env("CLICKHOUSE_ASYNC_INSERT", "false")
                .env("DISABLE_AUTH", "1")
                .env("DISK_BUFFER_DIR", buffer_dir.path())
                .stdout(stdout_file)
                .stderr(stderr_file)
                .spawn()
                .expect("Failed to start collector");

        let collector_pid = collector.id().expect("Failed to get collector PID");

        // Wait for ready
        let http_client = reqwest::Client::new();
        let health_url = format!("http://127.0.0.1:{}/health", health_port);

        let mut ready = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Ok(resp) = http_client.get(&health_url).send().await {
                if resp.status().is_success() {
                    ready = true;
                    break;
                }
            }
        }

        if !ready {
            let stdout = std::fs::read_to_string(&stdout_path).unwrap_or_default();
            let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
            panic!(
                "Collector failed to start\nstdout: {}\nstderr: {}",
                stdout, stderr
            );
        }

        Self {
            clickhouse,
            buffer_dir,
            log_dir,
            collector_pid,
            stdout_path,
            stderr_path,
            http_client,
            ch_client,
            traces_url: format!("http://127.0.0.1:{}/v1/traces", collector_port),
        }
    }

    async fn send_protobuf(&self, service_name: &str, span_name: &str, trace_id: &[u8]) -> reqwest::Response {
        let request = create_otlp_request(service_name, span_name, trace_id);
        let mut buf = Vec::new();
        request.encode(&mut buf).unwrap();

        self.http_client
            .post(&self.traces_url)
            .header("Content-Type", "application/x-protobuf")
            .header("X-Hive-Target-Ref", "org/project/target")
            .body(buf)
            .send()
            .await
            .expect("Failed to send request")
    }

    async fn send_json(&self, json: &str) -> reqwest::Response {
        self.http_client
            .post(&self.traces_url)
            .header("Content-Type", "application/json")
            .header("X-Hive-Target-Ref", "org/project/target")
            .body(json.to_string())
            .send()
            .await
            .expect("Failed to send request")
    }

    async fn wait_for_trace(&self, trace_id: &str, expected_count: u64) -> u64 {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct CountRow {
            count: u64,
        }

        // Poll for data to appear (max 5 seconds)
        for _ in 0..50 {
            let rows: Vec<CountRow> = self
                .ch_client
                .query("SELECT count() as count FROM default.otel_traces WHERE TraceId = ?")
                .bind(trace_id)
                .fetch_all()
                .await
                .expect("Failed to query");

            let count = rows.first().map(|r| r.count).unwrap_or(0);
            if count >= expected_count {
                return count;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Return final count after timeout
        let rows: Vec<CountRow> = self
            .ch_client
            .query("SELECT count() as count FROM default.otel_traces WHERE TraceId = ?")
            .bind(trace_id)
            .fetch_all()
            .await
            .expect("Failed to query");

        rows.first().map(|r| r.count).unwrap_or(0)
    }
}

fn create_otlp_request(service_name: &str, span_name: &str, trace_id: &[u8]) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(service_name.to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.to_vec(),
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    parent_span_id: vec![],
                    trace_state: String::new(),
                    name: span_name.to_string(),
                    kind: 2,
                    start_time_unix_nano: 1_750_000_000_000_000_000,
                    end_time_unix_nano: 1_750_000_001_000_000_000,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        message: String::new(),
                        code: 1,
                    }),
                    flags: 0,
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn test_e2e_protobuf_request() {
    let ctx = TestContext::new().await;

    let trace_id = random_trace_id();
    let trace_id_hex = bytes_to_hex(&trace_id);

    let response = ctx.send_protobuf("e2e-service", "e2e-operation", &trace_id).await;
    assert_eq!(response.status(), 200);

    assert_eq!(ctx.wait_for_trace(&trace_id_hex, 1).await, 1);
}

#[tokio::test]
async fn test_e2e_json_request() {
    let ctx = TestContext::new().await;

    let trace_id = random_trace_id();
    let trace_id_hex = bytes_to_hex(&trace_id);

    let json = format!(r#"{{
        "resourceSpans": [{{
            "resource": {{
                "attributes": [{{"key": "service.name", "value": {{"stringValue": "e2e-json-service"}}}}]
            }},
            "scopeSpans": [{{
                "spans": [{{
                    "traceId": "{}",
                    "spanId": "0102030405060708",
                    "name": "e2e-json-operation",
                    "kind": 2,
                    "startTimeUnixNano": "1750000000000000000",
                    "endTimeUnixNano": "1750000001000000000",
                    "status": {{"code": 1}}
                }}]
            }}]
        }}]
    }}"#, trace_id_hex);

    let response = ctx.send_json(&json).await;
    assert_eq!(response.status(), 200);

    assert_eq!(ctx.wait_for_trace(&trace_id_hex, 1).await, 1);
}

fn random_trace_id() -> Vec<u8> {
    (0..16).map(|_| rand::random::<u8>()).collect()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}