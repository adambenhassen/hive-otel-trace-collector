use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub enum SpanKind {
    Unspecified = 0,
    Internal = 1,
    Server = 2,
    Client = 3,
    Producer = 4,
    Consumer = 5,
}

impl SpanKind {
    pub fn from_i32(value: i32) -> Self {
        match value {
            1 => SpanKind::Internal,
            2 => SpanKind::Server,
            3 => SpanKind::Client,
            4 => SpanKind::Producer,
            5 => SpanKind::Consumer,
            _ => SpanKind::Unspecified,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            SpanKind::Unspecified => "SPAN_KIND_UNSPECIFIED",
            SpanKind::Internal => "SPAN_KIND_INTERNAL",
            SpanKind::Server => "SPAN_KIND_SERVER",
            SpanKind::Client => "SPAN_KIND_CLIENT",
            SpanKind::Producer => "SPAN_KIND_PRODUCER",
            SpanKind::Consumer => "SPAN_KIND_CONSUMER",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StatusCode {
    Unset = 0,
    Ok = 1,
    Error = 2,
}

impl StatusCode {
    pub fn from_i32(value: i32) -> Self {
        match value {
            1 => StatusCode::Ok,
            2 => StatusCode::Error,
            _ => StatusCode::Unset,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            StatusCode::Unset => "STATUS_CODE_UNSET",
            StatusCode::Ok => "STATUS_CODE_OK",
            StatusCode::Error => "STATUS_CODE_ERROR",
        }
    }
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct Span {
    #[serde(rename = "Timestamp")]
    pub timestamp: i64,

    #[serde(rename = "TraceId")]
    pub trace_id: String,

    #[serde(rename = "SpanId")]
    pub span_id: String,

    #[serde(rename = "ParentSpanId")]
    pub parent_span_id: String,

    #[serde(rename = "TraceState")]
    pub trace_state: String,

    #[serde(rename = "SpanName")]
    pub span_name: String,

    #[serde(rename = "SpanKind")]
    pub span_kind: String,

    #[serde(rename = "ServiceName")]
    pub service_name: String,

    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,

    #[serde(rename = "ScopeName")]
    pub scope_name: String,

    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,

    #[serde(rename = "SpanAttributes")]
    pub span_attributes: HashMap<String, String>,

    #[serde(rename = "Duration")]
    pub duration: u64,

    #[serde(rename = "StatusCode")]
    pub status_code: String,

    #[serde(rename = "StatusMessage")]
    pub status_message: String,

    #[serde(rename = "Events.Timestamp")]
    pub events_timestamp: Vec<i64>,

    #[serde(rename = "Events.Name")]
    pub events_name: Vec<String>,

    #[serde(rename = "Events.Attributes")]
    pub events_attributes: Vec<HashMap<String, String>>,

    #[serde(rename = "Links.TraceId")]
    pub links_trace_id: Vec<String>,

    #[serde(rename = "Links.SpanId")]
    pub links_span_id: Vec<String>,

    #[serde(rename = "Links.TraceState")]
    pub links_trace_state: Vec<String>,

    #[serde(rename = "Links.Attributes")]
    pub links_attributes: Vec<HashMap<String, String>>,
}

impl Span {
    pub fn new() -> Self {
        Self {
            timestamp: 0,
            trace_id: String::new(),
            span_id: String::new(),
            parent_span_id: String::new(),
            trace_state: String::new(),
            span_name: String::new(),
            span_kind: SpanKind::Unspecified.as_str().to_string(),
            service_name: String::new(),
            resource_attributes: HashMap::new(),
            scope_name: String::new(),
            scope_version: String::new(),
            span_attributes: HashMap::new(),
            duration: 0,
            status_code: StatusCode::Unset.as_str().to_string(),
            status_message: String::new(),
            events_timestamp: Vec::new(),
            events_name: Vec::new(),
            events_attributes: Vec::new(),
            links_trace_id: Vec::new(),
            links_span_id: Vec::new(),
            links_trace_state: Vec::new(),
            links_attributes: Vec::new(),
        }
    }
}

impl Default for Span {
    fn default() -> Self {
        Self::new()
    }
}

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

#[inline]
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        hex.push(HEX_CHARS[(byte >> 4) as usize] as char);
        hex.push(HEX_CHARS[(byte & 0x0f) as usize] as char);
    }
    hex
}
