use crate::receivers::vercel::LokiLogEntry;
use super::message::KafkaLogMessage;

pub fn from_loki_entries(entries: &[LokiLogEntry]) -> Vec<KafkaLogMessage> {
    entries.iter().map(KafkaLogMessage::from_loki_entry).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::vercel::LokiLabels;

    #[test]
    fn test_from_loki_entries() {
        let entries = vec![
            LokiLogEntry {
                timestamp_ns: 1702400000000000000,
                line: r#"{"message":"first"}"#.to_string(),
                labels: LokiLabels {
                    project_id: "prj_1".to_string(),
                    source: "lambda".to_string(),
                    log_type: "stdout".to_string(),
                    level: "info".to_string(),
                    target_id: "tenant_a".to_string(),
                },
            },
            LokiLogEntry {
                timestamp_ns: 1702400000000000001,
                line: r#"{"message":"second"}"#.to_string(),
                labels: LokiLabels {
                    project_id: "prj_2".to_string(),
                    source: "edge".to_string(),
                    log_type: "stderr".to_string(),
                    level: "error".to_string(),
                    target_id: "tenant_b".to_string(),
                },
            },
        ];

        let kafka_msgs = from_loki_entries(&entries);

        assert_eq!(kafka_msgs.len(), 2);
        assert_eq!(kafka_msgs[0].timestamp_ns, 1702400000000000000);
        assert_eq!(kafka_msgs[0].labels.target_id, "tenant_a");
        assert_eq!(kafka_msgs[1].timestamp_ns, 1702400000000000001);
        assert_eq!(kafka_msgs[1].labels.target_id, "tenant_b");
    }

    #[test]
    fn test_from_loki_entries_empty() {
        let entries: Vec<LokiLogEntry> = vec![];
        let kafka_msgs = from_loki_entries(&entries);
        assert!(kafka_msgs.is_empty());
    }
}
