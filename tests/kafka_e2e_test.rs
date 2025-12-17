use hmac::{Hmac, Mac};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::config::ClientConfig;
use rdkafka::Message;
use sha1::Sha1;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::kafka::{Kafka, KAFKA_PORT};

type HmacSha1 = Hmac<Sha1>;

const WEBHOOK_SECRET: &str = "test-webhook-secret";
const TEST_TOPIC: &str = "vercel-logs";

struct TestContext {
    #[allow(dead_code)]
    kafka: ContainerAsync<Kafka>,
    #[allow(dead_code)]
    log_dir: TempDir,
    collector_pid: u32,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    http_client: reqwest::Client,
    kafka_bootstrap: String,
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
        Self::new_with_topic_strategy("single", TEST_TOPIC).await
    }

    async fn new_with_topic_strategy(topic_strategy: &str, topic: &str) -> Self {
        // Start Kafka
        let kafka = Kafka::default()
            .start()
            .await
            .expect("Failed to start Kafka");

        let kafka_host = kafka.get_host().await.unwrap();
        let kafka_port = kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap();
        let kafka_bootstrap = format!("{}:{}", kafka_host, kafka_port);

        // Wait for Kafka to be ready
        tokio::time::sleep(Duration::from_secs(2)).await;

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
  kafka:
    bootstrap_servers: "{kafka_bootstrap}"
    topic: "{topic}"
    topic_strategy: "{topic_strategy}"
    topic_prefix: "logs-"
    serialization: "json"
    acks: "1"
    compression: "none"
    linger_ms: 0
    retries: 1

pipelines:
  logs:
    receiver: vercel
    exporter: kafka
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
            kafka,
            log_dir,
            collector_pid,
            stdout_path,
            stderr_path,
            http_client,
            kafka_bootstrap,
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

    async fn send_vercel_logs_with_target(&self, body: &str, target_id: &str) -> reqwest::Response {
        let signature = self.compute_signature(body.as_bytes());

        self.http_client
            .post(&self.logs_url)
            .header("Content-Type", "application/x-ndjson")
            .header("X-Vercel-Signature", signature)
            .header("X-Hive-Target-Ref", target_id)
            .body(body.to_string())
            .send()
            .await
            .expect("Failed to send request")
    }

    fn create_consumer(&self, topic: &str) -> BaseConsumer {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.kafka_bootstrap)
            .set("group.id", format!("test-group-{}", rand::random::<u32>()))
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Failed to create consumer");

        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        consumer
    }

    fn consume_messages(&self, consumer: &BaseConsumer, expected_count: usize) -> Vec<String> {
        let mut messages = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);

        while messages.len() < expected_count && std::time::Instant::now() < deadline {
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(msg)) => {
                    if let Some(payload) = msg.payload() {
                        if let Ok(text) = std::str::from_utf8(payload) {
                            messages.push(text.to_string());
                        }
                    }
                }
                Some(Err(e)) => {
                    eprintln!("Kafka consumer error: {}", e);
                }
                None => {}
            }
        }

        messages
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
async fn test_vercel_to_kafka_single_log() {
    let ctx = TestContext::new().await;

    let project_id = format!("prj_{}", rand::random::<u32>());
    let log_entry = make_vercel_log("log_1", "Hello from Vercel", &project_id);

    let response = ctx.send_vercel_logs(&log_entry).await;
    assert_eq!(response.status(), 200);

    // Wait for log to be flushed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Consume and verify
    let consumer = ctx.create_consumer(TEST_TOPIC);
    let messages = ctx.consume_messages(&consumer, 1);

    assert_eq!(messages.len(), 1, "Should have 1 message in Kafka");

    // Verify message content
    let msg: serde_json::Value = serde_json::from_str(&messages[0]).unwrap();
    assert!(msg["timestamp_ns"].is_number());
    // The line contains the transformed log JSON (id, message, deploymentId, etc)
    let line: serde_json::Value = serde_json::from_str(msg["line"].as_str().unwrap()).unwrap();
    assert_eq!(line["id"], "log_1");
    assert_eq!(line["message"], "Hello from Vercel");
    assert_eq!(line["deploymentId"], "dpl_test123");
    // Labels contain the indexed fields
    assert_eq!(msg["labels"]["project_id"], project_id);
    assert_eq!(msg["labels"]["source"], "lambda");
    assert_eq!(msg["labels"]["log_type"], "stdout");
}

#[tokio::test]
async fn test_vercel_to_kafka_ndjson_batch() {
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

    // Wait for logs to be flushed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Consume and verify
    let consumer = ctx.create_consumer(TEST_TOPIC);
    let messages = ctx.consume_messages(&consumer, 3);

    assert_eq!(messages.len(), 3, "Should have 3 messages in Kafka");

    // Verify all messages have the same project_id
    for msg_str in &messages {
        let msg: serde_json::Value = serde_json::from_str(msg_str).unwrap();
        assert_eq!(msg["labels"]["project_id"], project_id);
    }
}

#[tokio::test]
async fn test_vercel_to_kafka_per_tenant_topic() {
    let ctx = TestContext::new_with_topic_strategy("per_tenant", "default-logs").await;

    let target_id = format!("tenant_{}", rand::random::<u32>());
    let project_id = format!("prj_{}", rand::random::<u32>());
    let log_entry = make_vercel_log("log_1", "Per-tenant log", &project_id);

    let response = ctx.send_vercel_logs_with_target(&log_entry, &target_id).await;
    assert_eq!(response.status(), 200);

    // Wait for log to be flushed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Consume from tenant-specific topic
    let tenant_topic = format!("logs-{}", target_id);
    let consumer = ctx.create_consumer(&tenant_topic);
    let messages = ctx.consume_messages(&consumer, 1);

    assert_eq!(messages.len(), 1, "Should have 1 message in tenant topic");

    let msg: serde_json::Value = serde_json::from_str(&messages[0]).unwrap();
    assert_eq!(msg["labels"]["target_id"], target_id);
    assert_eq!(msg["labels"]["project_id"], project_id);
}

#[tokio::test]
async fn test_vercel_to_kafka_message_format() {
    let ctx = TestContext::new().await;

    let project_id = format!("prj_{}", rand::random::<u32>());
    let log_entry = serde_json::json!({
        "id": "log_format_test",
        "message": "Testing message format",
        "timestamp": current_timestamp_ms(),
        "type": "stderr",
        "source": "edge",
        "projectId": project_id,
        "deploymentId": "dpl_format_test",
        "level": "error",
        "statusCode": 500,
        "path": "/api/test",
        "host": "example.com"
    })
    .to_string();

    let response = ctx.send_vercel_logs(&log_entry).await;
    assert_eq!(response.status(), 200);

    // Wait for log to be flushed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Consume and verify full message structure
    let consumer = ctx.create_consumer(TEST_TOPIC);
    let messages = ctx.consume_messages(&consumer, 1);

    assert_eq!(messages.len(), 1);

    let msg: serde_json::Value = serde_json::from_str(&messages[0]).unwrap();

    // Verify structure
    assert!(msg["timestamp_ns"].is_number());
    assert!(msg["line"].is_string());
    assert!(msg["labels"].is_object());

    // Verify labels
    let labels = &msg["labels"];
    assert_eq!(labels["project_id"], project_id);
    assert_eq!(labels["source"], "edge");
    assert_eq!(labels["log_type"], "stderr");
    assert_eq!(labels["level"], "error");
    assert_eq!(labels["target_id"], "org/project/target");

    // Verify original log data is preserved in line
    let line: serde_json::Value = serde_json::from_str(msg["line"].as_str().unwrap()).unwrap();
    assert_eq!(line["message"], "Testing message format");
    assert_eq!(line["statusCode"], 500);
    assert_eq!(line["path"], "/api/test");
}

#[tokio::test]
async fn test_vercel_invalid_signature_kafka() {
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
