use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;

pub fn parse(body: &[u8], is_json: bool) -> Result<ExportTraceServiceRequest, String> {
    if is_json {
        parse_json(body)
    } else {
        parse_protobuf(body)
    }
}

fn parse_json(body: &[u8]) -> Result<ExportTraceServiceRequest, String> {
    // simd-json requires mutable buffer for in-place parsing
    let mut body_vec = body.to_vec();

    // Try simd-json first (SIMD-accelerated), fall back to serde_json
    match simd_json::serde::from_slice(&mut body_vec) {
        Ok(request) => Ok(request),
        Err(simd_err) => serde_json::from_slice(body).map_err(|e| {
            format!(
                "Failed to parse JSON OTLP (simd: {}, serde: {})",
                simd_err, e
            )
        }),
    }
}

fn parse_protobuf(body: &[u8]) -> Result<ExportTraceServiceRequest, String> {
    ExportTraceServiceRequest::decode(body)
        .map_err(|e| format!("Failed to parse protobuf OTLP: {}", e))
}

