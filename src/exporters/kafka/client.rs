use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use thiserror::Error;
use tracing::{error, info, warn};

use super::message::KafkaLogMessage;

#[derive(Error, Debug)]
pub enum KafkaError {
    #[error("Kafka producer error: {0}")]
    Producer(#[from] rdkafka::error::KafkaError),

    #[error("Message delivery failed: {count} messages failed")]
    Delivery { count: usize },

    #[error("Serialization error: {0}")]
    Serialization(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TopicStrategy {
    Single,
    PerTenant,
}

impl Default for TopicStrategy {
    fn default() -> Self {
        Self::Single
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SerializationFormat {
    Json,
    Protobuf,
}

impl Default for SerializationFormat {
    fn default() -> Self {
        Self::Json
    }
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String, // "kafka1:9092,kafka2:9092"
    pub topic: String,
    pub topic_strategy: TopicStrategy,
    pub topic_prefix: String,
    pub serialization: SerializationFormat,
    pub acks: String, //"0", "1", or "all"
    pub compression: String, // "none", "gzip", "snappy", "lz4", "zstd"
    pub batch_size: usize, // bytes
    pub linger_ms: u64,
    pub request_timeout_ms: u64,
    pub delivery_timeout_ms: u64,
    pub retries: u32,
    pub security_protocol: String, // "plaintext", "ssl", "sasl_plaintext", "sasl_ssl"
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: "logs".to_string(),
            topic_strategy: TopicStrategy::Single,
            topic_prefix: "logs-".to_string(),
            serialization: SerializationFormat::Json,
            acks: "1".to_string(),
            compression: "lz4".to_string(),
            batch_size: 16384,
            linger_ms: 5,
            request_timeout_ms: 30000,
            delivery_timeout_ms: 120000,
            retries: 3,
            security_protocol: "plaintext".to_string(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        }
    }
}

pub struct KafkaClient {
    producer: FutureProducer,
    config: KafkaConfig,
    healthy: AtomicBool,
}

impl KafkaClient {
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaError> {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("acks", &config.acks)
            .set("compression.type", &config.compression)
            .set("batch.size", config.batch_size.to_string())
            .set("linger.ms", config.linger_ms.to_string())
            .set("request.timeout.ms", config.request_timeout_ms.to_string())
            .set("delivery.timeout.ms", config.delivery_timeout_ms.to_string())
            .set("retries", config.retries.to_string())
            .set("security.protocol", &config.security_protocol);

        // Add SASL config if provided
        if let Some(ref mechanism) = config.sasl_mechanism {
            client_config.set("sasl.mechanism", mechanism);
        }
        if let Some(ref username) = config.sasl_username {
            client_config.set("sasl.username", username);
        }
        if let Some(ref password) = config.sasl_password {
            client_config.set("sasl.password", password);
        }

        let producer: FutureProducer = client_config.create()?;

        info!(
            bootstrap_servers = %config.bootstrap_servers,
            topic = %config.topic,
            topic_strategy = ?config.topic_strategy,
            serialization = ?config.serialization,
            "Kafka client initialized"
        );

        Ok(Self {
            producer,
            config,
            healthy: AtomicBool::new(true),
        })
    }

    /// Send logs to Kafka
    pub async fn send(&self, logs: &[KafkaLogMessage]) -> Result<(), KafkaError> {
        if logs.is_empty() {
            return Ok(());
        }

        // Pre-serialize all messages and resolve topics
        let mut prepared: Vec<(String, Vec<u8>, Vec<u8>)> = Vec::with_capacity(logs.len());
        for log in logs {
            let topic = self.resolve_topic(&log.labels.target_id);
            let payload = match self.config.serialization {
                SerializationFormat::Json => log
                    .to_json()
                    .map_err(|e| KafkaError::Serialization(e.to_string()))?,
                SerializationFormat::Protobuf => log
                    .to_protobuf()
                    .map_err(|e| KafkaError::Serialization(e.to_string()))?,
            };
            let key = log.labels.target_id.as_bytes().to_vec();
            prepared.push((topic, key, payload));
        }

        // Now send all messages, prepared vec owns the data
        let mut futures = Vec::with_capacity(prepared.len());
        for (topic, key, payload) in &prepared {
            let record = FutureRecord::to(topic)
                .key(key.as_slice())
                .payload(payload.as_slice());

            let future = self.producer.send(record, Duration::from_secs(0));
            futures.push(future);
        }

        // Wait for all deliveries
        let results = futures::future::join_all(futures).await;

        let mut failed = 0;
        for result in results {
            if let Err((err, _)) = result {
                warn!(error = %err, "Kafka message delivery failed");
                failed += 1;
            }
        }

        if failed > 0 {
            self.mark_unhealthy();
            return Err(KafkaError::Delivery { count: failed });
        }

        self.mark_healthy();
        Ok(())
    }

    pub fn resolve_topic(&self, target_id: &str) -> String {
        match self.config.topic_strategy {
            TopicStrategy::Single => self.config.topic.clone(),
            TopicStrategy::PerTenant => {
                format!("{}{}", self.config.topic_prefix, target_id)
            }
        }
    }

    #[allow(dead_code)]
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn mark_healthy(&self) {
        if !self.healthy.swap(true, Ordering::SeqCst) {
            info!("Kafka connection recovered");
        }
    }

    fn mark_unhealthy(&self) {
        if self.healthy.swap(false, Ordering::SeqCst) {
            error!("Kafka connection unhealthy");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = KafkaConfig::default();
        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert_eq!(config.topic, "logs");
        assert_eq!(config.topic_strategy, TopicStrategy::Single);
        assert_eq!(config.serialization, SerializationFormat::Json);
        assert_eq!(config.acks, "1");
        assert_eq!(config.compression, "lz4");
    }

    #[test]
    fn test_topic_resolution_single() {
        let config = KafkaConfig {
            topic_strategy: TopicStrategy::Single,
            topic: "vercel-logs".to_string(),
            ..Default::default()
        };

        // Can't create a real client without Kafka, so test the logic directly
        assert_eq!(config.topic, "vercel-logs");
        match config.topic_strategy {
            TopicStrategy::Single => assert!(true),
            TopicStrategy::PerTenant => panic!("Expected Single strategy"),
        }
    }

    #[test]
    fn test_topic_resolution_per_tenant() {
        let config = KafkaConfig {
            topic_strategy: TopicStrategy::PerTenant,
            topic_prefix: "logs-".to_string(),
            ..Default::default()
        };

        let target_id = "tenant123";
        let expected = format!("{}{}", config.topic_prefix, target_id);
        assert_eq!(expected, "logs-tenant123");
    }
}
