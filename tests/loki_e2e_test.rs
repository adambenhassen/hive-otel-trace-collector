use hmac::{Hmac, Mac};
use sha1::Sha1;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ContainerAsync, GenericImage};

type HmacSha1 = Hmac<Sha1>;

const WEBHOOK_SECRET: &str = "test-webhook-secret";

struct TestContext {
    #[allow(dead_code)]
    loki: ContainerAsync<GenericImage>,
    #[allow(dead_code)]
    log_dir: TempDir,
    collector_pid: u32,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    http_client: reqwest::Client,
    loki_url: String,
    logs_url: String,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.collector_pid as i32, libc::SIGKILL);
        }

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
        // Start Loki
        let loki = GenericImage::new("grafana/loki", "3.0.0")
            .with_exposed_port(3100.into())
            .with_wait_for(testcontainers::core::WaitFor::message_on_stderr("Loki started"))
            .start()
            .await
            .expect("Failed to start Loki");

        let loki_host = loki.get_host().await.unwrap();
        let loki_port = loki.get_host_port_ipv4(3100).await.unwrap();
        let loki_url = format!("http://{}:{}", loki_host, loki_port);

        // Wait for Loki to be ready
        let http_client = reqwest::Client::new();
        let ready_url = format!("{}/ready", loki_url);
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Ok(resp) = http_client.get(&ready_url).send().await {
                if resp.status().is_success() {
                    break;
                }
            }
        }

        // Find available ports
        let collector_port = portpicker::pick_unused_port().expect("No free port");
        let health_port = portpicker::pick_unused_port().expect("No free port");

        let log_dir = tempfile::tempdir().expect("Failed to create log dir");
        let stdout_path = log_dir.path().join("stdout.log");
        let stderr_path = log_dir.path().join("stderr.log");

        let stdout_file = std::fs::File::create(&stdout_path).expect("Failed to create stdout log");
        let stderr_file = std::fs::File::create(&stderr_path).expect("Failed to create stderr log");

        // Create config file
        let config_path = log_dir.path().join("config.yaml");
        let config_content = format!(
            r#"
server:
  port: {collector_port}
  health_port: {health_port}

receivers:
  vercel:
    webhook_secret: "{secret}"

exporters:
  loki:
    url: "{loki_url}"

pipelines:
  logs:
    receiver: vercel
    exporter: loki
    batch:
      max_size: 100
      timeout_ms: 100
      workers: 1
"#,
            secret = WEBHOOK_SECRET
        );
        std::fs::write(&config_path, config_content).expect("Failed to write config file");

        // Start collector
        let collector =
            tokio::process::Command::new(env!("CARGO_BIN_EXE_hive-otel-trace-collector"))
                .arg("--config")
                .arg(&config_path)
                .stdout(stdout_file)
                .stderr(stderr_file)
                .spawn()
                .expect("Failed to start collector");

        let collector_pid = collector.id().expect("Failed to get collector PID");

        // Wait for ready
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
            loki,
            log_dir,
            collector_pid,
            stdout_path,
            stderr_path,
            http_client,
            loki_url,
            logs_url: format!("http://127.0.0.1:{}/v1/logs/vercel", collector_port),
        }
    }

    fn compute_signature(&self, body: &[u8]) -> String {
        let mut mac = HmacSha1::new_from_slice(WEBHOOK_SECRET.as_bytes()).unwrap();
        mac.update(body);
        format!("sha1={}", hex::encode(mac.finalize().into_bytes()))
    }

    async fn send_vercel_logs(&self, body: &str) -> reqwest::Response {
        let signature = self.compute_signature(body.as_bytes());

        self.http_client
            .post(&self.logs_url)
            .header("Content-Type", "application/x-ndjson")
            .header("X-Vercel-Signature", signature)
            .header("X-Hive-Target-Ref", "org/project/target")
            .body(body.to_string())
            .send()
            .await
            .expect("Failed to send request")
    }

    async fn query_loki(&self, query: &str) -> Option<serde_json::Value> {
        let now = chrono::Utc::now();
        let start = (now - chrono::Duration::hours(1)).timestamp_nanos_opt().unwrap_or(0);
        let end = now.timestamp_nanos_opt().unwrap_or(0);

        let url = format!(
            "{}/loki/api/v1/query_range?query={}&start={}&end={}",
            self.loki_url,
            urlencoding::encode(query),
            start,
            end
        );

        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .expect("Failed to query Loki");

        if !resp.status().is_success() {
            return None;
        }

        resp.json().await.ok()
    }

    async fn wait_for_logs(&self, project_id: &str, expected_count: usize) -> usize {
        let query = format!(r#"{{project_id="{}"}}"#, project_id);

        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;

            if let Some(result) = self.query_loki(&query).await {
                if let Some(streams) = result["data"]["result"].as_array() {
                    let count: usize = streams
                        .iter()
                        .filter_map(|s| s["values"].as_array())
                        .map(|v| v.len())
                        .sum();

                    if count >= expected_count {
                        return count;
                    }
                }
            }
        }

        // Return final count
        self.query_loki(&query)
            .await
            .and_then(|result| {
                result["data"]["result"].as_array().map(|streams| {
                    streams
                        .iter()
                        .filter_map(|s| s["values"].as_array())
                        .map(|v| v.len())
                        .sum()
                })
            })
            .unwrap_or(0)
    }
}

fn current_timestamp_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn make_vercel_log(id: &str, message: &str, project_id: &str) -> String {
    serde_json::json!({
        "id": id,
        "message": message,
        "timestamp": current_timestamp_ms(),
        "type": "stdout",
        "source": "lambda",
        "projectId": project_id,
        "deploymentId": "dpl_test123"
    })
    .to_string()
}

#[tokio::test]
async fn test_vercel_to_loki_single_log() {
    let ctx = TestContext::new().await;

    let project_id = format!("prj_{}", rand::random::<u32>());
    let log_entry = make_vercel_log("log_1", "Hello from Vercel", &project_id);

    let response = ctx.send_vercel_logs(&log_entry).await;
    assert_eq!(response.status(), 200);

    let count = ctx.wait_for_logs(&project_id, 1).await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_vercel_to_loki_ndjson_batch() {
    let ctx = TestContext::new().await;

    let project_id = format!("prj_{}", rand::random::<u32>());
    let logs = vec![
        make_vercel_log("log_1", "First log", &project_id),
        make_vercel_log("log_2", "Second log", &project_id),
        make_vercel_log("log_3", "Third log", &project_id),
    ];
    let ndjson = logs.join("\n");

    let response = ctx.send_vercel_logs(&ndjson).await;
    assert_eq!(response.status(), 200);

    let count = ctx.wait_for_logs(&project_id, 3).await;
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_vercel_invalid_signature() {
    let ctx = TestContext::new().await;

    let log_entry = make_vercel_log("log_1", "Test", "prj_test");

    let response = ctx
        .http_client
        .post(&ctx.logs_url)
        .header("Content-Type", "application/x-ndjson")
        .header("X-Vercel-Signature", "sha1=invalid")
        .header("X-Hive-Target-Ref", "org/project/target")
        .body(log_entry)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn test_vercel_missing_signature() {
    let ctx = TestContext::new().await;

    let log_entry = make_vercel_log("log_1", "Test", "prj_test");

    let response = ctx
        .http_client
        .post(&ctx.logs_url)
        .header("Content-Type", "application/x-ndjson")
        .header("X-Hive-Target-Ref", "org/project/target")
        .body(log_entry)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn test_vercel_labels_preserved() {
    let ctx = TestContext::new().await;

    let project_id = format!("prj_{}", rand::random::<u32>());

    // Send stdout log
    let stdout_log = serde_json::json!({
        "id": "log_stdout",
        "message": "stdout message",
        "timestamp": current_timestamp_ms(),
        "type": "stdout",
        "source": "lambda",
        "projectId": project_id,
        "deploymentId": "dpl_test"
    })
    .to_string();

    let response = ctx.send_vercel_logs(&stdout_log).await;
    assert_eq!(response.status(), 200);

    // Wait for logs to arrive
    let count = ctx.wait_for_logs(&project_id, 1).await;
    assert_eq!(count, 1, "Should have 1 log");

    // Query with specific labels
    let query = format!(
        r#"{{project_id="{}", source="lambda", log_type="stdout"}}"#,
        project_id
    );
    let result = ctx.query_loki(&query).await.expect("Should get query result");

    let streams = result["data"]["result"].as_array().unwrap();
    assert!(!streams.is_empty(), "Should find logs with matching labels");

    // Verify stream labels
    let stream = &streams[0]["stream"];
    assert_eq!(stream["project_id"], project_id);
    assert_eq!(stream["source"], "lambda");
    assert_eq!(stream["log_type"], "stdout");
    assert_eq!(stream["level"], "info");
    assert_eq!(stream["target_id"], "org/project/target");
}
