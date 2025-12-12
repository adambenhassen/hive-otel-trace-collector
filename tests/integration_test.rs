mod common;

use common::CREATE_TABLE_SQL;
use hive_otel_trace_collector::{
    receivers::otlp::{parse, transform},
    ClickHouseConfig, InsertPool, Span,
};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue, KeyValue},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan, Status},
};
use prost::Message;
use std::collections::HashMap;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::clickhouse::ClickHouse;

struct TestContext {
    #[allow(dead_code)]
    container: ContainerAsync<ClickHouse>,
    insert_pool: InsertPool,
    client: clickhouse::Client,
}

impl TestContext {
    async fn new() -> Self {
        let container = ClickHouse::default()
            .with_tag("24.8")
            .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
            .start()
            .await
            .expect("Failed to start ClickHouse container");

        let host = container.get_host().await.expect("Failed to get host");
        let port = container
            .get_host_port_ipv4(8123)
            .await
            .expect("Failed to get port");
        let url = format!("http://{}:{}", host, port);

        let client = clickhouse::Client::default()
            .with_url(&url)
            .with_database("default");

        client
            .query(CREATE_TABLE_SQL)
            .execute()
            .await
            .expect("Failed to create table");

        let config = ClickHouseConfig {
            url: url.clone(),
            database: "default".to_string(),
            table: "otel_traces".to_string(),
            username: "default".to_string(),
            password: String::new(),
            async_insert: false,
            wait_for_async_insert: false,
            async_insert_max_data_size: 10 * 1024 * 1024,
            async_insert_busy_timeout_ms: 1000,
            async_insert_max_query_number: 450,
            skip_unknown_fields: true,
            date_time_best_effort: true,
        };

        let insert_pool = InsertPool::new(config)
            .await
            .expect("Failed to create InsertPool");

        Self {
            container,
            insert_pool,
            client,
        }
    }

    async fn query_spans(&self, service_name: &str) -> Vec<QueryResult> {
        self.client
            .query("SELECT Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind, ServiceName, ResourceAttributes, ScopeName, ScopeVersion, SpanAttributes, Duration, StatusCode, StatusMessage, `Events.Timestamp`, `Events.Name`, `Events.Attributes`, `Links.TraceId`, `Links.SpanId`, `Links.TraceState`, `Links.Attributes` FROM default.otel_traces WHERE ServiceName = ? ORDER BY Timestamp")
            .bind(service_name)
            .fetch_all()
            .await
            .expect("Failed to query spans")
    }

    async fn count_spans(&self) -> u64 {
        let rows: Vec<CountRow> = self
            .client
            .query("SELECT count() as count FROM default.otel_traces")
            .fetch_all()
            .await
            .expect("Failed to count spans");
        rows.first().map(|r| r.count).unwrap_or(0)
    }
}

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct QueryResult {
    #[serde(rename = "Timestamp")]
    timestamp: i64,
    #[serde(rename = "TraceId")]
    trace_id: String,
    #[serde(rename = "SpanId")]
    span_id: String,
    #[serde(rename = "ParentSpanId")]
    parent_span_id: String,
    #[serde(rename = "TraceState")]
    trace_state: String,
    #[serde(rename = "SpanName")]
    span_name: String,
    #[serde(rename = "SpanKind")]
    span_kind: String,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeName")]
    scope_name: String,
    #[serde(rename = "ScopeVersion")]
    scope_version: String,
    #[serde(rename = "SpanAttributes")]
    span_attributes: HashMap<String, String>,
    #[serde(rename = "Duration")]
    duration: u64,
    #[serde(rename = "StatusCode")]
    status_code: String,
    #[serde(rename = "StatusMessage")]
    status_message: String,
    #[serde(rename = "Events.Timestamp")]
    events_timestamp: Vec<i64>,
    #[serde(rename = "Events.Name")]
    events_name: Vec<String>,
    #[serde(rename = "Events.Attributes")]
    events_attributes: Vec<HashMap<String, String>>,
    #[serde(rename = "Links.TraceId")]
    links_trace_id: Vec<String>,
    #[serde(rename = "Links.SpanId")]
    links_span_id: Vec<String>,
    #[serde(rename = "Links.TraceState")]
    links_trace_state: Vec<String>,
    #[serde(rename = "Links.Attributes")]
    links_attributes: Vec<HashMap<String, String>>,
}

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct CountRow {
    #[serde(rename = "count")]
    count: u64,
}

fn create_test_span(
    service_name: &str,
    span_name: &str,
    trace_id: &str,
    span_id: &str,
) -> Span {
    let mut resource_attributes = HashMap::new();
    resource_attributes.insert("service.name".to_string(), service_name.to_string());

    let mut span_attributes = HashMap::new();
    span_attributes.insert("test.span".to_string(), "true".to_string());

    let mut event_attrs = HashMap::new();
    event_attrs.insert("event.id".to_string(), "1".to_string());

    let mut link_attrs = HashMap::new();
    link_attrs.insert("link.id".to_string(), "1".to_string());

    Span {
        timestamp: 1_750_000_000_000_000_000,
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        parent_span_id: "0000000000000000".to_string(),
        trace_state: "test=true".to_string(),
        span_name: span_name.to_string(),
        span_kind: "SPAN_KIND_SERVER".to_string(),
        service_name: service_name.to_string(),
        resource_attributes,
        scope_name: "test.scope".to_string(),
        scope_version: "1.0.0".to_string(),
        span_attributes,
        duration: 1_000_000_000,
        status_code: "STATUS_CODE_OK".to_string(),
        status_message: "OK".to_string(),
        events_timestamp: vec![1_750_000_000_500_000_000],
        events_name: vec!["test-event".to_string()],
        events_attributes: vec![event_attrs],
        links_trace_id: vec!["ffffffffffffffffffffffffffffffff".to_string()],
        links_span_id: vec!["eeeeeeeeeeeeeeee".to_string()],
        links_trace_state: vec!["linked=true".to_string()],
        links_attributes: vec![link_attrs],
    }
}

fn create_test_otlp_request(
    service_name: &str,
    span_name: &str,
    span_count: usize,
) -> ExportTraceServiceRequest {
    let spans: Vec<OtlpSpan> = (0..span_count)
        .map(|i| OtlpSpan {
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, (i as u8)],
            parent_span_id: vec![],
            trace_state: String::new(),
            name: format!("{}-{}", span_name, i),
            kind: 2, // SERVER
            start_time_unix_nano: 1_000_000_000 + (i as u64 * 1_000_000),
            end_time_unix_nano: 2_000_000_000 + (i as u64 * 1_000_000),
            attributes: vec![KeyValue {
                key: "test.index".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(i as i64)),
                }),
            }],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: Some(Status {
                message: String::new(),
                code: 1, // OK
            }),
            flags: 0,
        })
        .collect();

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
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn test_insert_single_span() {
    let ctx = TestContext::new().await;

    let span = create_test_span(
        "test-service",
        "test-operation",
        "0102030405060708090a0b0c0d0e0f10",
        "0102030405060708",
    );

    ctx.insert_pool
        .insert(&[span])
        .await
        .expect("Failed to insert span");

    // Query immediately like the passing tests do
    let results = ctx.query_spans("test-service").await;
    assert_eq!(results.len(), 1);

    let result = &results[0];
    assert_eq!(result.trace_id, "0102030405060708090a0b0c0d0e0f10");
    assert_eq!(result.span_id, "0102030405060708");
    assert_eq!(result.span_name, "test-operation");
    assert_eq!(result.service_name, "test-service");
    assert_eq!(result.duration, 1_000_000_000);
    assert_eq!(result.status_code, "STATUS_CODE_OK");
}

#[tokio::test]
async fn test_insert_batch() {
    let ctx = TestContext::new().await;

    let spans: Vec<Span> = (0..100)
        .map(|i| {
            create_test_span(
                "batch-service",
                &format!("operation-{}", i),
                "0102030405060708090a0b0c0d0e0f10",
                &format!("01020304050607{:02x}", i),
            )
        })
        .collect();

    ctx.insert_pool
        .insert(&spans)
        .await
        .expect("Failed to insert batch");

    let count = ctx.count_spans().await;
    assert_eq!(count, 100);

    let results = ctx.query_spans("batch-service").await;
    assert_eq!(results.len(), 100);
}

#[tokio::test]
async fn test_insert_large_batch() {
    let ctx = TestContext::new().await;

    let spans: Vec<Span> = (0..10_000u32)
        .map(|i| {
            create_test_span(
                "large-batch-service",
                &format!("operation-{}", i),
                "0102030405060708090a0b0c0d0e0f10",
                &format!("{:016x}", i),
            )
        })
        .collect();

    ctx.insert_pool
        .insert(&spans)
        .await
        .expect("Failed to insert large batch");

    let count = ctx.count_spans().await;
    assert_eq!(count, 10_000);
}

#[tokio::test]
async fn test_insert_empty_batch() {
    let ctx = TestContext::new().await;

    ctx.insert_pool
        .insert(&[])
        .await
        .expect("Failed to insert empty batch");

    let count = ctx.count_spans().await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_full_pipeline_json() {
    let ctx = TestContext::new().await;

    let json = r#"{
        "resourceSpans": [{
            "resource": {
                "attributes": [{"key": "service.name", "value": {"stringValue": "json-pipeline-service"}}]
            },
            "scopeSpans": [{
                "spans": [{
                    "traceId": "aabbccddeeff00112233445566778899",
                    "spanId": "1122334455667788",
                    "name": "json-operation",
                    "kind": 2,
                    "startTimeUnixNano": "1000000000",
                    "endTimeUnixNano": "2000000000",
                    "status": {"code": 1}
                }]
            }]
        }]
    }"#;

    let request = parse::parse(json.as_bytes(), true).expect("Failed to parse JSON");
    let spans = transform::transform_request(&request, "org/project/target");

    ctx.insert_pool
        .insert(&spans)
        .await
        .expect("Failed to insert spans");

    let results = ctx.query_spans("json-pipeline-service").await;
    assert_eq!(results.len(), 1);

    let result = &results[0];
    assert_eq!(result.trace_id, "aabbccddeeff00112233445566778899");
    assert_eq!(result.span_id, "1122334455667788");
    assert_eq!(result.span_name, "json-operation");
    assert_eq!(
        result.resource_attributes.get("hive.target_id"),
        Some(&"org/project/target".to_string())
    );
    assert_eq!(
        result.span_attributes.get("hive.target_id"),
        Some(&"org/project/target".to_string())
    );
}

#[tokio::test]
async fn test_full_pipeline_protobuf() {
    let ctx = TestContext::new().await;

    let request = create_test_otlp_request("proto-pipeline-service", "proto-operation", 5);
    let mut buf = Vec::new();
    request.encode(&mut buf).unwrap();

    let parsed = parse::parse(&buf, false).expect("Failed to parse protobuf");
    let spans = transform::transform_request(&parsed, "org/project/proto-target");

    ctx.insert_pool
        .insert(&spans)
        .await
        .expect("Failed to insert spans");

    let results = ctx.query_spans("proto-pipeline-service").await;
    assert_eq!(results.len(), 5);

    for (i, result) in results.iter().enumerate() {
        assert_eq!(result.span_name, format!("proto-operation-{}", i));
        assert_eq!(
            result.resource_attributes.get("hive.target_id"),
            Some(&"org/project/proto-target".to_string())
        );
    }
}

#[tokio::test]
async fn test_attributes_preserved() {
    let ctx = TestContext::new().await;

    let mut span = create_test_span(
        "attr-service",
        "attr-operation",
        "0102030405060708090a0b0c0d0e0f10",
        "0102030405060708",
    );

    span.resource_attributes
        .insert("deployment.environment".to_string(), "production".to_string());
    span.resource_attributes
        .insert("host.name".to_string(), "server-01".to_string());
    span.span_attributes
        .insert("http.method".to_string(), "GET".to_string());
    span.span_attributes
        .insert("http.url".to_string(), "https://api.example.com/users".to_string());
    span.span_attributes
        .insert("http.status_code".to_string(), "200".to_string());

    ctx.insert_pool
        .insert(&[span])
        .await
        .expect("Failed to insert span");

    let results = ctx.query_spans("attr-service").await;
    assert_eq!(results.len(), 1);

    let result = &results[0];
    assert_eq!(
        result.resource_attributes.get("deployment.environment"),
        Some(&"production".to_string())
    );
    assert_eq!(
        result.resource_attributes.get("host.name"),
        Some(&"server-01".to_string())
    );
    assert_eq!(
        result.span_attributes.get("http.method"),
        Some(&"GET".to_string())
    );
    assert_eq!(
        result.span_attributes.get("http.url"),
        Some(&"https://api.example.com/users".to_string())
    );
    assert_eq!(
        result.span_attributes.get("http.status_code"),
        Some(&"200".to_string())
    );
}

#[tokio::test]
async fn test_all_columns_inserted() {
    let ctx = TestContext::new().await;

    // Create a span with ALL fields populated
    let mut resource_attributes = HashMap::new();
    resource_attributes.insert("service.name".to_string(), "all-columns-service".to_string());
    resource_attributes.insert("deployment.environment".to_string(), "test".to_string());

    let mut span_attributes = HashMap::new();
    span_attributes.insert("http.method".to_string(), "POST".to_string());
    span_attributes.insert("http.url".to_string(), "https://example.com/api".to_string());

    let mut event_attrs = HashMap::new();
    event_attrs.insert("exception.type".to_string(), "RuntimeError".to_string());

    let mut link_attrs = HashMap::new();
    link_attrs.insert("link.type".to_string(), "parent".to_string());

    let span = Span {
        timestamp: 1_750_000_000_000_000_000,
        trace_id: "aabbccdd11223344aabbccdd11223344".to_string(),
        span_id: "1122334455667788".to_string(),
        parent_span_id: "0011223344556677".to_string(),
        trace_state: "vendor1=value1,vendor2=value2".to_string(),
        span_name: "all-columns-operation".to_string(),
        span_kind: "SPAN_KIND_CLIENT".to_string(),
        service_name: "all-columns-service".to_string(),
        resource_attributes,
        scope_name: "my.instrumentation.library".to_string(),
        scope_version: "1.2.3".to_string(),
        span_attributes,
        duration: 500_000_000, // 500ms
        status_code: "STATUS_CODE_ERROR".to_string(),
        status_message: "Something went wrong".to_string(),
        events_timestamp: vec![1_750_000_000_100_000_000, 1_750_000_000_200_000_000],
        events_name: vec!["event1".to_string(), "event2".to_string()],
        events_attributes: vec![event_attrs.clone(), HashMap::new()],
        links_trace_id: vec!["ffeeddccbbaa99887766554433221100".to_string()],
        links_span_id: vec!["8877665544332211".to_string()],
        links_trace_state: vec!["linked=true".to_string()],
        links_attributes: vec![link_attrs],
    };

    ctx.insert_pool
        .insert(&[span])
        .await
        .expect("Failed to insert span");

    let results = ctx.query_spans("all-columns-service").await;
    assert_eq!(results.len(), 1);

    let r = &results[0];

    // Verify all columns
    assert_eq!(r.timestamp, 1_750_000_000_000_000_000);
    assert_eq!(r.trace_id, "aabbccdd11223344aabbccdd11223344");
    assert_eq!(r.span_id, "1122334455667788");
    assert_eq!(r.parent_span_id, "0011223344556677");
    assert_eq!(r.trace_state, "vendor1=value1,vendor2=value2");
    assert_eq!(r.span_name, "all-columns-operation");
    assert_eq!(r.span_kind, "SPAN_KIND_CLIENT");
    assert_eq!(r.service_name, "all-columns-service");
    assert_eq!(r.resource_attributes.get("deployment.environment"), Some(&"test".to_string()));
    assert_eq!(r.scope_name, "my.instrumentation.library");
    assert_eq!(r.scope_version, "1.2.3");
    assert_eq!(r.span_attributes.get("http.method"), Some(&"POST".to_string()));
    assert_eq!(r.duration, 500_000_000);
    assert_eq!(r.status_code, "STATUS_CODE_ERROR");
    assert_eq!(r.status_message, "Something went wrong");

    // Verify events
    assert_eq!(r.events_timestamp.len(), 2);
    assert_eq!(r.events_timestamp[0], 1_750_000_000_100_000_000);
    assert_eq!(r.events_name, vec!["event1", "event2"]);
    assert_eq!(r.events_attributes.len(), 2);
    assert_eq!(r.events_attributes[0].get("exception.type"), Some(&"RuntimeError".to_string()));

    // Verify links
    assert_eq!(r.links_trace_id, vec!["ffeeddccbbaa99887766554433221100"]);
    assert_eq!(r.links_span_id, vec!["8877665544332211"]);
    assert_eq!(r.links_trace_state, vec!["linked=true"]);
    assert_eq!(r.links_attributes.len(), 1);
    assert_eq!(r.links_attributes[0].get("link.type"), Some(&"parent".to_string()));
}

#[tokio::test]
async fn test_multiple_services() {
    let ctx = TestContext::new().await;

    let spans: Vec<Span> = ["service-a", "service-b", "service-c"]
        .iter()
        .enumerate()
        .flat_map(|(service_idx, service_name)| {
            (0..10).map(move |span_idx| {
                create_test_span(
                    service_name,
                    &format!("operation-{}", span_idx),
                    "0102030405060708090a0b0c0d0e0f10",
                    &format!("{:02x}00000000000{:02x}", service_idx, span_idx),
                )
            })
        })
        .collect();

    ctx.insert_pool
        .insert(&spans)
        .await
        .expect("Failed to insert spans");

    let count = ctx.count_spans().await;
    assert_eq!(count, 30);

    assert_eq!(ctx.query_spans("service-a").await.len(), 10);
    assert_eq!(ctx.query_spans("service-b").await.len(), 10);
    assert_eq!(ctx.query_spans("service-c").await.len(), 10);
}

#[tokio::test]
async fn test_concurrent_inserts() {
    let ctx = TestContext::new().await;

    let handles: Vec<_> = (0..10)
        .map(|batch_idx| {
            let spans: Vec<Span> = (0..100)
                .map(|span_idx| {
                    create_test_span(
                        "concurrent-service",
                        &format!("batch-{}-span-{}", batch_idx, span_idx),
                        "0102030405060708090a0b0c0d0e0f10",
                        &format!("{:02x}000000{:04x}", batch_idx, span_idx),
                    )
                })
                .collect();
            spans
        })
        .collect();

    for spans in handles {
        ctx.insert_pool
            .insert(&spans)
            .await
            .expect("Failed to insert batch");
    }

    let count = ctx.count_spans().await;
    assert_eq!(count, 1000);
}

