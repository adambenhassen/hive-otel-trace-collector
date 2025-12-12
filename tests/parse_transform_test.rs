use hive_otel_trace_collector::receivers::otlp::{parse, transform};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue, KeyValue},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use prost::Message;

fn create_test_otlp_request(
    service_name: &str,
    span_name: &str,
    span_count: usize,
) -> ExportTraceServiceRequest {
    let spans: Vec<Span> = (0..span_count)
        .map(|i| Span {
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, (i as u8)],
            parent_span_id: vec![],
            trace_state: String::new(),
            name: format!("{}-{}", span_name, i),
            kind: 2, // SERVER
            start_time_unix_nano: 1000000000 + (i as u64 * 1000000),
            end_time_unix_nano: 2000000000 + (i as u64 * 1000000),
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

#[test]
fn test_parse_json_valid() {
    let json = r#"{
        "resourceSpans": [{
            "resource": {
                "attributes": [{"key": "service.name", "value": {"stringValue": "test-service"}}]
            },
            "scopeSpans": [{
                "spans": [{
                    "traceId": "01020304050607080910111213141516",
                    "spanId": "0102030405060708",
                    "name": "test-span",
                    "kind": 1,
                    "startTimeUnixNano": "1000000000",
                    "endTimeUnixNano": "2000000000"
                }]
            }]
        }]
    }"#;

    let result = parse::parse(json.as_bytes(), true);
    assert!(result.is_ok());

    let request = result.unwrap();
    assert_eq!(request.resource_spans.len(), 1);
    assert_eq!(request.resource_spans[0].scope_spans[0].spans.len(), 1);
}

#[test]
fn test_parse_json_invalid() {
    let invalid_json = "not valid json at all";
    let result = parse::parse(invalid_json.as_bytes(), true);
    assert!(result.is_err());
}

#[test]
fn test_parse_json_empty_spans() {
    let json = r#"{"resourceSpans": []}"#;
    let result = parse::parse(json.as_bytes(), true);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().resource_spans.len(), 0);
}

#[test]
fn test_parse_protobuf_valid() {
    let request = create_test_otlp_request("test-service", "test-span", 1);
    let mut buf = Vec::new();
    request.encode(&mut buf).unwrap();

    let result = parse::parse(&buf, false);
    assert!(result.is_ok());
}

#[test]
fn test_parse_protobuf_invalid() {
    let invalid_proto = vec![0xff, 0xff, 0xff, 0xff];
    let result = parse::parse(&invalid_proto, false);
    assert!(result.is_err());
}

#[test]
fn test_transform_single_span() {
    let request = create_test_otlp_request("my-service", "my-operation", 1);
    let target_id = "org/project/target";

    let rows = transform::transform_request(&request, target_id);

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.service_name, "my-service");
    assert_eq!(row.span_name, "my-operation-0");
    assert_eq!(row.span_kind, "SPAN_KIND_SERVER");
    assert_eq!(row.status_code, "STATUS_CODE_OK");

    // Verify target_id is injected
    assert_eq!(
        row.resource_attributes.get("hive.target_id"),
        Some(&target_id.to_string())
    );
    assert_eq!(
        row.span_attributes.get("hive.target_id"),
        Some(&target_id.to_string())
    );
}

#[test]
fn test_transform_multiple_spans() {
    let request = create_test_otlp_request("batch-service", "batch-op", 100);
    let target_id = "org/project/target";

    let rows = transform::transform_request(&request, target_id);

    assert_eq!(rows.len(), 100);

    // Verify each span has correct target_id
    for row in &rows {
        assert_eq!(row.service_name, "batch-service");
        assert_eq!(
            row.resource_attributes.get("hive.target_id"),
            Some(&target_id.to_string())
        );
    }
}

#[test]
fn test_transform_empty_request() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![],
    };
    let rows = transform::transform_request(&request, "target");
    assert!(rows.is_empty());
}

#[test]
fn test_transform_preserves_attributes() {
    let request = create_test_otlp_request("attr-service", "attr-span", 1);
    let rows = transform::transform_request(&request, "target");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    // The test span has test.index attribute
    assert_eq!(
        row.span_attributes.get("test.index"),
        Some(&"0".to_string())
    );
}

#[test]
fn test_transform_calculates_duration() {
    let request = create_test_otlp_request("duration-service", "duration-span", 1);
    let rows = transform::transform_request(&request, "target");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    // Duration should be end_time - start_time
    // 2000000000 - 1000000000 = 1000000000 ns = 1 second
    assert_eq!(row.duration, 1000000000);
}

#[test]
fn test_transform_hex_trace_id() {
    let request = create_test_otlp_request("hex-service", "hex-span", 1);
    let rows = transform::transform_request(&request, "target");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    // trace_id bytes [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16] as hex
    assert_eq!(row.trace_id, "0102030405060708090a0b0c0d0e0f10");
}

#[test]
fn test_transform_large_span_with_many_attributes() {
    let large_value = "x".repeat(5000); // 5KB attribute value

    let mut attributes = vec![KeyValue {
        key: "test.index".to_string(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(0)),
        }),
    }];

    for i in 0..3 {
        attributes.push(KeyValue {
            key: format!("large.attr.{}", i),
            value: Some(AnyValue {
                value: Some(Value::StringValue(large_value.clone())),
            }),
        });
    }

    let span = Span {
        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
        parent_span_id: vec![],
        trace_state: String::new(),
        name: "large-span".to_string(),
        kind: 2,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes,
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
    };

    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("large-service".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![span],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    // Test 1: With no limit (0), large span should be accepted
    let rows = transform::transform_request_for_test(&request, "target", 0);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].span_name, "large-span");
    assert_eq!(rows[0].service_name, "large-service");
    assert!(rows[0].span_attributes.len() >= 4);

    // Test 2: With 1KB limit, this ~15KB span should be dropped
    let rows = transform::transform_request_for_test(&request, "target", 1 * 1024);
    assert_eq!(rows.len(), 0, "Large span (15KB+) should be filtered with 1KB limit");

    // Test 3: With 20KB limit, large span should be accepted
    let rows = transform::transform_request_for_test(&request, "target", 20 * 1024);
    assert_eq!(rows.len(), 1, "Large span should pass with generous 20KB limit");
}

#[test]
fn test_transform_multiple_spans_mixed_sizes() {
    // Create 100 small spans
    let request = create_test_otlp_request("mixed-service", "mixed-span", 100);

    // Test 1: With no limit (0), all spans should pass
    let rows = transform::transform_request_for_test(&request, "target", 0);
    assert_eq!(rows.len(), 100);

    // Test 2: With 10KB limit, all small spans should still pass
    let rows = transform::transform_request_for_test(&request, "target", 10 * 1024);
    assert_eq!(rows.len(), 100);

    // Test 3: With very tight 1 byte limit, all spans should be dropped
    let rows = transform::transform_request_for_test(&request, "target", 1);
    assert_eq!(rows.len(), 0, "All spans should be filtered with 1B limit");
}
