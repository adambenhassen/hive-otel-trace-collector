use prost::Message;
use serde::{Deserialize, Serialize};

use crate::receivers::vercel::{LokiLabels, LokiLogEntry};

// internal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaLogMessage {
    pub timestamp_ns: i64,
    pub line: String,
    pub labels: LokiLabels,
}

impl KafkaLogMessage {
    pub fn from_loki_entry(entry: &LokiLogEntry) -> Self {
        Self {
            timestamp_ns: entry.timestamp_ns,
            line: entry.line.clone(),
            labels: entry.labels.clone(),
        }
    }

    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn to_protobuf(&self) -> Result<Vec<u8>, prost::EncodeError> {
        let proto = KafkaLogMessageProto {
            timestamp_ns: self.timestamp_ns,
            line: self.line.clone(),
            labels: Some(LabelsProto {
                project_id: self.labels.project_id.clone(),
                source: self.labels.source.clone(),
                log_type: self.labels.log_type.clone(),
                level: self.labels.level.clone(),
                target_id: self.labels.target_id.clone(),
            }),
        };
        let mut buf = Vec::with_capacity(proto.encoded_len());
        proto.encode(&mut buf)?;
        Ok(buf)
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct KafkaLogMessageProto {
    #[prost(int64, tag = "1")]
    pub timestamp_ns: i64,

    #[prost(string, tag = "2")]
    pub line: String,

    #[prost(message, optional, tag = "3")]
    pub labels: Option<LabelsProto>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LabelsProto {
    #[prost(string, tag = "1")]
    pub project_id: String,

    #[prost(string, tag = "2")]
    pub source: String,

    #[prost(string, tag = "3")]
    pub log_type: String,

    #[prost(string, tag = "4")]
    pub level: String,

    #[prost(string, tag = "5")]
    pub target_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_message() -> KafkaLogMessage {
        KafkaLogMessage {
            timestamp_ns: 1702400000000000000,
            line: r#"{"id":"log_123","message":"Hello world"}"#.to_string(),
            labels: LokiLabels {
                project_id: "prj_abc".to_string(),
                source: "lambda".to_string(),
                log_type: "stdout".to_string(),
                level: "info".to_string(),
                target_id: "tenant_123".to_string(),
            },
        }
    }

    #[test]
    fn test_to_json() {
        let msg = create_test_message();
        let json = msg.to_json().unwrap();
        let parsed: KafkaLogMessage = serde_json::from_slice(&json).unwrap();

        assert_eq!(parsed.timestamp_ns, msg.timestamp_ns);
        assert_eq!(parsed.line, msg.line);
        assert_eq!(parsed.labels.project_id, msg.labels.project_id);
        assert_eq!(parsed.labels.target_id, msg.labels.target_id);
    }

    #[test]
    fn test_to_protobuf() {
        let msg = create_test_message();
        let proto_bytes = msg.to_protobuf().unwrap();

        // Decode and verify
        let decoded = KafkaLogMessageProto::decode(proto_bytes.as_slice()).unwrap();
        assert_eq!(decoded.timestamp_ns, msg.timestamp_ns);
        assert_eq!(decoded.line, msg.line);

        let labels = decoded.labels.unwrap();
        assert_eq!(labels.project_id, msg.labels.project_id);
        assert_eq!(labels.target_id, msg.labels.target_id);
    }

    #[test]
    fn test_from_loki_entry() {
        let loki_entry = LokiLogEntry {
            timestamp_ns: 1702400000000000000,
            line: r#"{"message":"test"}"#.to_string(),
            labels: LokiLabels {
                project_id: "prj_123".to_string(),
                source: "edge".to_string(),
                log_type: "stderr".to_string(),
                level: "error".to_string(),
                target_id: "tenant_456".to_string(),
            },
        };

        let kafka_msg = KafkaLogMessage::from_loki_entry(&loki_entry);

        assert_eq!(kafka_msg.timestamp_ns, loki_entry.timestamp_ns);
        assert_eq!(kafka_msg.line, loki_entry.line);
        assert_eq!(kafka_msg.labels.project_id, loki_entry.labels.project_id);
        assert_eq!(kafka_msg.labels.target_id, loki_entry.labels.target_id);
    }
}
