use crate::proto::span::{bytes_to_hex, Span, SpanKind, StatusCode};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue, KeyValue},
};
use std::collections::HashMap;

const SERVICE_NAME_KEY: &str = "service.name";
const HIVE_TARGET_ID_KEY: &str = "hive.target_id";

pub fn transform_request(request: &ExportTraceServiceRequest, target_id: &str) -> Vec<Span> {
    let mut rows = Vec::new();

    for resource_spans in &request.resource_spans {
        // Extract resource attributes and add target_id
        let (mut resource_attrs, service_name) =
            extract_resource_attributes(resource_spans.resource.as_ref());
        resource_attrs.insert(HIVE_TARGET_ID_KEY.to_string(), target_id.to_string());

        for scope_spans in &resource_spans.scope_spans {
            // Extract scope info
            let (scope_name, scope_version) = scope_spans
                .scope
                .as_ref()
                .map(|s| (s.name.clone(), s.version.clone()))
                .unwrap_or_default();

            for span in &scope_spans.spans {
                let row = transform_span(
                    span,
                    &resource_attrs,
                    &service_name,
                    &scope_name,
                    &scope_version,
                    target_id,
                );
                rows.push(row);
            }
        }
    }

    rows
}

fn transform_span(
    span: &opentelemetry_proto::tonic::trace::v1::Span,
    resource_attrs: &HashMap<String, String>,
    service_name: &str,
    scope_name: &str,
    scope_version: &str,
    target_id: &str,
) -> Span {
    // Convert trace/span IDs from bytes to hex
    let trace_id = bytes_to_hex(&span.trace_id);
    let span_id = bytes_to_hex(&span.span_id);
    let parent_span_id = bytes_to_hex(&span.parent_span_id);

    // Calculate duration in nanoseconds
    let duration = span
        .end_time_unix_nano
        .saturating_sub(span.start_time_unix_nano);

    // Extract span attributes and add target_id
    let mut span_attrs = extract_attributes(&span.attributes);
    span_attrs.insert(HIVE_TARGET_ID_KEY.to_string(), target_id.to_string());

    // Extract status
    let (status_code, status_message) = span
        .status
        .as_ref()
        .map(|s| {
            (
                StatusCode::from_i32(s.code).as_str().to_string(),
                s.message.clone(),
            )
        })
        .unwrap_or_else(|| (StatusCode::Unset.as_str().to_string(), String::new()));

    // Extract events
    let (events_ts, events_name, events_attrs) = extract_events(&span.events);

    // Extract links
    let (links_trace_id, links_span_id, links_trace_state, links_attrs) =
        extract_links(&span.links);

    Span {
        timestamp: span.start_time_unix_nano as i64,
        trace_id,
        span_id,
        parent_span_id,
        trace_state: span.trace_state.clone(),
        span_name: span.name.clone(),
        span_kind: SpanKind::from_i32(span.kind).as_str().to_string(),
        service_name: service_name.to_string(),
        resource_attributes: resource_attrs.clone(),
        scope_name: scope_name.to_string(),
        scope_version: scope_version.to_string(),
        span_attributes: span_attrs,
        duration,
        status_code,
        status_message,
        events_timestamp: events_ts,
        events_name,
        events_attributes: events_attrs,
        links_trace_id,
        links_span_id,
        links_trace_state,
        links_attributes: links_attrs,
    }
}

fn extract_resource_attributes(
    resource: Option<&opentelemetry_proto::tonic::resource::v1::Resource>,
) -> (HashMap<String, String>, String) {
    let mut attrs = HashMap::new();
    let mut service_name = String::new();

    if let Some(res) = resource {
        for kv in &res.attributes {
            if let Some(value) = extract_attribute_value(&kv.value) {
                if kv.key == SERVICE_NAME_KEY {
                    service_name = value.clone();
                }
                attrs.insert(kv.key.clone(), value);
            }
        }
    }

    (attrs, service_name)
}

fn extract_attributes(attributes: &[KeyValue]) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(attributes.len());
    for kv in attributes {
        if let Some(value) = extract_attribute_value(&kv.value) {
            map.insert(kv.key.clone(), value);
        }
    }
    map
}

fn extract_attribute_value(value: &Option<AnyValue>) -> Option<String> {
    value.as_ref().and_then(|v| {
        v.value.as_ref().map(|val| match val {
            Value::StringValue(s) => s.clone(),
            Value::BoolValue(b) => b.to_string(),
            Value::IntValue(i) => i.to_string(),
            Value::DoubleValue(d) => d.to_string(),
            Value::ArrayValue(arr) => {
                // Serialize array as JSON for complex types
                let items: Vec<String> = arr
                    .values
                    .iter()
                    .filter_map(|v| extract_attribute_value(&Some(v.clone())))
                    .collect();
                serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
            }
            Value::KvlistValue(kv) => {
                // Serialize key-value list as JSON
                let map: HashMap<String, String> = kv
                    .values
                    .iter()
                    .filter_map(|kv| {
                        extract_attribute_value(&kv.value).map(|v| (kv.key.clone(), v))
                    })
                    .collect();
                serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
            }
            Value::BytesValue(b) => bytes_to_hex(b),
        })
    })
}

fn extract_events(
    events: &[opentelemetry_proto::tonic::trace::v1::span::Event],
) -> (Vec<i64>, Vec<String>, Vec<HashMap<String, String>>) {
    let mut timestamps = Vec::with_capacity(events.len());
    let mut names = Vec::with_capacity(events.len());
    let mut attrs = Vec::with_capacity(events.len());

    for event in events {
        timestamps.push(event.time_unix_nano as i64);
        names.push(event.name.clone());
        attrs.push(extract_attributes(&event.attributes));
    }

    (timestamps, names, attrs)
}

fn extract_links(
    links: &[opentelemetry_proto::tonic::trace::v1::span::Link],
) -> (
    Vec<String>,
    Vec<String>,
    Vec<String>,
    Vec<HashMap<String, String>>,
) {
    let mut trace_ids = Vec::with_capacity(links.len());
    let mut span_ids = Vec::with_capacity(links.len());
    let mut trace_states = Vec::with_capacity(links.len());
    let mut attrs = Vec::with_capacity(links.len());

    for link in links {
        trace_ids.push(bytes_to_hex(&link.trace_id));
        span_ids.push(bytes_to_hex(&link.span_id));
        trace_states.push(link.trace_state.clone());
        attrs.push(extract_attributes(&link.attributes));
    }

    (trace_ids, span_ids, trace_states, attrs)
}

